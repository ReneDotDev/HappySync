package index

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/happysync/happysync/internal/model"
	"golang.org/x/text/unicode/norm"
)

type ScanOptions struct {
	Ignore            []string
	Executable        string
	Previous          map[string]model.Baseline
	PreviousManifests map[string]model.Manifest
	Force             bool
}
type ignoreRule struct {
	base                string
	pattern             *regexp.Regexp
	negative, directory bool
}

func parseRule(base, line string) (ignoreRule, bool, error) {
	line = strings.TrimSuffix(line, "\r")
	for strings.HasSuffix(line, " ") {
		slashes := 0
		for i := len(line) - 2; i >= 0 && line[i] == '\\'; i-- {
			slashes++
		}
		if slashes%2 == 1 {
			break
		}
		line = strings.TrimSuffix(line, " ")
	}
	if line == "" || strings.HasPrefix(line, "#") {
		return ignoreRule{}, false, nil
	}
	r := ignoreRule{base: base}
	if strings.HasPrefix(line, "!") {
		r.negative = true
		line = line[1:]
	}
	if strings.HasPrefix(line, "\\#") || strings.HasPrefix(line, "\\!") {
		line = line[1:]
	}
	r.directory = strings.HasSuffix(line, "/")
	line = strings.TrimSuffix(line, "/")
	anchored := strings.HasPrefix(line, "/")
	line = strings.TrimPrefix(line, "/")
	if line == "" {
		return r, false, nil
	}
	var b strings.Builder
	if anchored || strings.Contains(line, "/") {
		b.WriteString("^")
	} else {
		b.WriteString("(?:^|/)")
	}
	for i := 0; i < len(line); i++ {
		switch line[i] {
		case '*':
			if i+1 < len(line) && line[i+1] == '*' {
				i++
				if i+1 < len(line) && line[i+1] == '/' {
					i++
					b.WriteString("(?:.*/)?")
				} else {
					b.WriteString(".*")
				}
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '[':
			end := strings.IndexByte(line[i+1:], ']')
			if end < 0 {
				b.WriteString("\\[")
			} else {
				cls := line[i+1 : i+1+end]
				if strings.HasPrefix(cls, "!") {
					cls = "^" + cls[1:]
				}
				b.WriteString("[" + cls + "]")
				i += end + 1
			}
		case '\\':
			if i+1 < len(line) {
				i++
				b.WriteString(regexp.QuoteMeta(line[i : i+1]))
			}
		default:
			b.WriteString(regexp.QuoteMeta(line[i : i+1]))
		}
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return r, false, fmt.Errorf("invalid ignore pattern: %w", err)
	}
	r.pattern = re
	return r, true, nil
}
func readRules(root, base string) ([]ignoreRule, error) {
	var rules []ignoreRule
	// An advertised path may have a local symlink ancestor. Never load ignore
	// files through it while deciding whether a remote entry should be applied.
	current := root
	if base != "" {
		for _, component := range strings.Split(base, "/") {
			current = filepath.Join(current, component)
			info, err := os.Lstat(current)
			if os.IsNotExist(err) {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return nil, errors.New("symlink in ignore directory")
			}
			if !info.IsDir() {
				return nil, nil
			}
		}
	}
	for _, name := range []string{".gitignore", ".happysyncignore"} {
		path := filepath.Join(root, filepath.FromSlash(base), name)
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if info.Size() > 1<<20 {
			return nil, errors.New("ignore file exceeds 1 MiB")
		}
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		opened, err := file.Stat()
		if err != nil || !os.SameFile(info, opened) {
			file.Close()
			return nil, errors.New("ignore file changed during open")
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 4096), 65536)
		for scanner.Scan() {
			r, ok, err := parseRule(norm.NFC.String(base), scanner.Text())
			if err != nil {
				file.Close()
				return nil, err
			}
			if ok {
				rules = append(rules, r)
			}
		}
		err = scanner.Err()
		file.Close()
		if err != nil {
			return nil, err
		}
	}
	return rules, nil
}
func defaultRules(extra []string) ([]ignoreRule, error) {
	var rules []ignoreRule
	for _, line := range append([]string{".git/", "node_modules/"}, extra...) {
		r, ok, err := parseRule("", line)
		if err != nil {
			return nil, err
		}
		if ok {
			rules = append(rules, r)
		}
	}
	return rules, nil
}
func matchesRules(rules []ignoreRule, p string, directory bool) bool {
	ignored := false
	for _, r := range rules {
		relative := p
		if r.base != "" {
			if !strings.HasPrefix(p, r.base+"/") {
				continue
			}
			relative = strings.TrimPrefix(p, r.base+"/")
		}
		parts := strings.Split(relative, "/")
		for n := 1; n <= len(parts); n++ {
			if r.directory && n == len(parts) && !directory {
				continue
			}
			if r.pattern.MatchString(strings.Join(parts[:n], "/")) {
				ignored = !r.negative
				break
			}
		}
	}
	return ignored
}

// Ignored checks rules for a remote projected path as well as all its parents.
// Ignored parent directories cannot be reopened by a child negation, matching git.
func Ignored(root, p string, directory bool, options ...ScanOptions) (bool, error) {
	if err := ValidatePath(p); err != nil {
		return true, err
	}
	opt := ScanOptions{}
	if len(options) > 0 {
		opt = options[0]
	}
	rules, err := defaultRules(nil)
	if err != nil {
		return false, err
	}
	extra, err := defaultRules(opt.Ignore)
	if err != nil {
		return false, err
	}
	extra = extra[2:]
	parts := strings.Split(p, "/")
	for n := 0; n < len(parts); n++ {
		base := strings.Join(parts[:n], "/")
		more, err := readRules(root, base)
		if err != nil {
			return false, err
		}
		rules = append(rules, more...)
		current := strings.Join(parts[:n+1], "/")
		if matchesRules(append(append([]ignoreRule(nil), rules...), extra...), current, n < len(parts)-1 || directory) {
			return true, nil
		}
	}
	if opt.Executable != "" {
		// This is an identity exclusion, not a filesystem lookup. In particular,
		// deleted descendants can sit below an ancestor now replaced by a file.
		// Their logical paths must still be checked without traversing that file.
		target, _ := filepath.Abs(filepath.Join(root, filepath.FromSlash(p)))
		a, _ := filepath.Abs(opt.Executable)
		if resolved, err := filepath.EvalSymlinks(a); err == nil {
			a = resolved
		}
		if norm.NFC.String(target) == norm.NFC.String(a) {
			return true, nil
		}
	}
	return false, nil
}

// Scan inventories files without following symlinks; a collision or unstable
// file aborts the snapshot, so absence is never misinterpreted as deletion.
func Scan(root string, options ...ScanOptions) (map[string]model.Baseline, map[string]model.Manifest, []string, error) {
	opt := ScanOptions{}
	if len(options) > 0 {
		opt = options[0]
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, nil, nil, err
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, nil, nil, err
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, nil, nil, errors.New("sync root is not a real directory")
	}
	// Canonicalize parent aliases (e.g. macOS /var -> /private/var) after
	// rejecting a symlink at the root itself.
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, nil, nil, err
	}
	executable := opt.Executable
	if executable != "" {
		executable, _ = filepath.Abs(executable)
		if resolved, err := filepath.EvalSymlinks(executable); err == nil {
			executable = resolved
		}
	}
	rules, err := defaultRules(nil)
	if err != nil {
		return nil, nil, nil, err
	}
	extras, err := defaultRules(opt.Ignore)
	if err != nil {
		return nil, nil, nil, err
	}
	extras = extras[2:]
	directoryRules := map[string][]ignoreRule{}
	activeRules := map[string][]ignoreRule{}
	baselines := map[string]model.Baseline{}
	manifests := map[string]model.Manifest{}
	seen := map[string]string{}
	var warnings []string
	err = filepath.WalkDir(root, func(physical string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, physical)
		if err != nil {
			return err
		}
		p := norm.NFC.String(filepath.ToSlash(relative))
		if p == "." {
			more, err := readRules(root, "")
			if err != nil {
				return err
			}
			rules = append(rules, more...)
			directoryRules["."] = rules
			activeRules["."] = append(append([]ignoreRule(nil), rules...), extras...)
			return nil
		}
		if FoldPath(d.Name()) == FoldPath(".HappySync") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		parent := path.Dir(p)
		active := activeRules[parent]
		if matchesRules(active, p, d.IsDir()) || physical == executable {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			warnings = append(warnings, fmt.Sprintf("skipped symlink: %s", p))
			return nil
		}
		if err := ValidatePath(p); err != nil {
			return fmt.Errorf("unsupported path %q: %w", p, err)
		}
		key := FoldPath(p)
		if previous, ok := seen[key]; ok {
			return fmt.Errorf("portable filename collision between %q and %q", previous, relative)
		}
		seen[key] = relative
		info, err := d.Info()
		if err != nil {
			return err
		}
		if d.IsDir() {
			baselines[p] = model.Baseline{Directory: true, Mtime: info.ModTime().UnixNano()}
			more, err := readRules(root, filepath.ToSlash(relative))
			if err != nil {
				return err
			}
			directoryRules[p] = append(append([]ignoreRule(nil), directoryRules[parent]...), more...)
			activeRules[p] = append(append([]ignoreRule(nil), directoryRules[p]...), extras...)
			return nil
		}
		if !info.Mode().IsRegular() {
			warnings = append(warnings, fmt.Sprintf("skipped special file: %s", p))
			return nil
		}
		m, exists := opt.PreviousManifests[p]
		old := opt.Previous[p]
		if opt.Force || !exists || old.Directory || old.Size != info.Size() || old.Mtime != info.ModTime().UnixNano() {
			m, err = BuildManifest(physical)
			if err != nil {
				return fmt.Errorf("hash %q: %w", p, err)
			}
		}
		baselines[p] = model.Baseline{Hash: m.Hash, Size: m.Size, Mtime: info.ModTime().UnixNano()}
		manifests[p] = m
		return nil
	})
	if err != nil {
		return nil, nil, warnings, err
	}
	return baselines, manifests, warnings, nil
}
