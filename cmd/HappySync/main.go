package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/happysync/happysync/internal/config"
	"github.com/happysync/happysync/internal/daemon"
	"github.com/happysync/happysync/internal/engine"
	"github.com/happysync/happysync/internal/network"
	"github.com/happysync/happysync/internal/status"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

var version = "dev"

type stringsFlag []string

func (s *stringsFlag) String() string     { return strings.Join(*s, ",") }
func (s *stringsFlag) Set(v string) error { *s = append(*s, v); return nil }

type options struct {
	folder, key, keyFile, listen, stateDir                                        string
	stdin, background, worker, json, mdnsOff, dhtOff, discovery, relay, serverDHT bool
	peers, bootstrap, relays, servers, ignores, advertise                         stringsFlag
	workers                                                                       int
	staging                                                                       int64
	scan                                                                          time.Duration
	relayTotalRate, relayPeerRate, relayDataLimit                                 int64
	relayDuration                                                                 time.Duration
	maxReservations, maxCircuits, maxRelayStreams                                 int
	discoveryMaxGroups, discoveryMaxRegistrations, discoveryMaxPerPeer            int
	allowPeers                                                                    stringsFlag
	uploadRate, downloadRate                                                      int64
	uploadRateSet, downloadRateSet                                                bool
}
type ready struct {
	Key    string        `json:"key"`
	Status engine.Status `json:"status"`
	Error  string        `json:"error,omitempty"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		for _, arg := range os.Args[1:] {
			if arg == "--internal-worker" {
				// Startup failures travel over the readiness pipe so a detached
				// launcher can preserve the actionable cause without printing a key.
				json.NewEncoder(os.Stdout).Encode(ready{Error: status.Redact(err.Error())})
				break
			}
		}
		fmt.Fprintln(os.Stderr, "HappySync:", status.Redact(err.Error()))
		os.Exit(1)
	}
}
func parse(args []string) (string, options, error) {
	command := "start"
	if len(args) > 0 {
		switch args[0] {
		case "start", "status", "stop", "serve", "version":
			command = args[0]
			args = args[1:]
		}
	}
	var o options
	f := flag.NewFlagSet("HappySync "+command, flag.ContinueOnError)
	f.SetOutput(status.RedactingWriter{Writer: os.Stderr})
	f.StringVar(&o.folder, "folder", ".", "sync root (default: current directory)")
	f.StringVar(&o.key, "key", "", "secret share invitation")
	f.StringVar(&o.keyFile, "key-file", "", "read invitation from file")
	f.BoolVar(&o.stdin, "key-stdin", false, "read invitation from stdin")
	f.BoolVar(&o.background, "background", false, "detach after readiness confirmation")
	f.BoolVar(&o.worker, "internal-worker", false, "internal worker mode")
	f.BoolVar(&o.json, "json", false, "JSON status")
	f.StringVar(&o.listen, "listen", "", "client multiaddr, or server host:port")
	f.BoolVar(&o.mdnsOff, "no-mdns", false, "disable LAN discovery")
	f.BoolVar(&o.dhtOff, "no-dht", false, "disable public DHT discovery")
	f.Var(&o.peers, "peer", "manual peer multiaddr (repeatable)")
	f.Var(&o.bootstrap, "bootstrap", "DHT bootstrap multiaddr (repeatable)")
	f.Var(&o.relays, "relay-peer", "relay server multiaddr (repeatable)")
	f.Var(&o.servers, "discovery-server", "custom discovery multiaddr (repeatable)")
	f.Var(&o.ignores, "ignore", "gitignore-style exclusion (repeatable)")
	f.IntVar(&o.workers, "workers", 8, "concurrent block downloads (1..64)")
	f.Int64Var(&o.staging, "staging-limit", 8<<30, "maximum cached block bytes")
	f.DurationVar(&o.scan, "scan-interval", 10*time.Minute, "comprehensive hashing interval")
	f.Int64Var(&o.uploadRate, "upload-rate", 0, "aggregate sync upload bytes per second (0: unlimited; persisted)")
	f.Int64Var(&o.downloadRate, "download-rate", 0, "aggregate sync download bytes per second (0: unlimited; persisted)")
	f.BoolVar(&o.discovery, "discovery", false, "enable server discovery")
	f.BoolVar(&o.relay, "relay", false, "enable circuit relay v2 server")
	f.BoolVar(&o.serverDHT, "dht-server", false, "enable DHT server")
	f.StringVar(&o.stateDir, "state-dir", ".HappySync-server", "server identity directory")
	f.Var(&o.advertise, "advertise", "server public multiaddr (repeatable)")
	f.Var(&o.allowPeers, "allow-peer", "allowed relay client Peer ID (repeatable; requires both endpoints)")
	f.Int64Var(&o.relayTotalRate, "relay-total-rate", 32<<20, "relay aggregate forwarded bytes per second")
	f.Int64Var(&o.relayPeerRate, "relay-peer-rate", 8<<20, "relay bytes per second per endpoint")
	f.Int64Var(&o.relayDataLimit, "relay-data-limit", 256<<20, "relay bytes per circuit session")
	f.DurationVar(&o.relayDuration, "relay-duration", 5*time.Minute, "maximum circuit session duration")
	f.IntVar(&o.maxReservations, "max-reservations", 64, "maximum relay reservations")
	f.IntVar(&o.maxCircuits, "max-circuits", 4, "maximum circuits per peer")
	f.IntVar(&o.maxRelayStreams, "max-relay-streams", 512, "maximum server streams")
	f.IntVar(&o.discoveryMaxGroups, "discovery-max-groups", 1024, "maximum opaque discovery groups")
	f.IntVar(&o.discoveryMaxRegistrations, "discovery-max-registrations", 4096, "maximum discovery registrations")
	f.IntVar(&o.discoveryMaxPerPeer, "discovery-max-per-peer", 8, "maximum registrations per identity")
	if e := f.Parse(args); e != nil {
		return command, o, e
	}
	if f.NArg() != 0 {
		return command, o, errors.New("unexpected positional arguments")
	}
	f.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "upload-rate":
			o.uploadRateSet = true
		case "download-rate":
			o.downloadRateSet = true
		}
	})
	if o.uploadRate < 0 || o.downloadRate < 0 {
		return command, o, errors.New("bandwidth limits cannot be negative")
	}
	if len(o.allowPeers) > 256 {
		return command, o, errors.New("too many allowed relay peers")
	}
	for _, id := range o.allowPeers {
		if _, err := peer.Decode(id); err != nil {
			return command, o, errors.New("invalid allowed relay Peer ID")
		}
	}
	return command, o, nil
}
func run(args []string) error {
	command, o, err := parse(args)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if command == "version" {
		fmt.Println("HappySync", version)
		return nil
	}
	if command == "serve" {
		return serve(o)
	}
	root, err := filepath.Abs(o.folder)
	if err != nil {
		return err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("cannot resolve sync folder: %w", err)
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return errors.New("sync folder must exist and be a directory")
	}
	o.folder = root
	if command == "status" || command == "stop" {
		return control(command, o)
	}
	count := 0
	if o.key != "" {
		count++
	}
	if o.keyFile != "" {
		count++
	}
	if o.stdin {
		count++
	}
	if count > 1 {
		return errors.New("choose only one of --key, --key-file, --key-stdin")
	}
	if o.keyFile != "" {
		f, e := os.Open(o.keyFile)
		if e != nil {
			return errors.New("cannot read key file")
		}
		b, e := io.ReadAll(io.LimitReader(f, 4097))
		f.Close()
		if e != nil || len(b) > 4096 {
			return errors.New("invalid key file")
		}
		o.key = strings.TrimSpace(string(b))
	}
	if o.stdin {
		b, e := io.ReadAll(io.LimitReader(os.Stdin, 4097))
		if e != nil || len(b) > 4096 {
			return errors.New("invalid key input")
		}
		o.key = strings.TrimSpace(string(b))
	}
	if count > 0 {
		if _, _, e := config.DecodeKey(o.key); e != nil {
			return e
		}
	}
	if o.workers < 1 || o.workers > 64 || o.staging < 0 || o.scan < time.Second {
		return errors.New("invalid worker, staging or scan limits")
	}
	if o.background && !o.worker {
		return launch(o)
	}
	return start(o)
}
func workerArgs(o options) []string {
	a := []string{"start", "--internal-worker", "--folder", o.folder, "--workers", fmt.Sprint(o.workers), "--staging-limit", fmt.Sprint(o.staging), "--scan-interval", o.scan.String()}
	if o.key != "" {
		a = append(a, "--key-stdin")
	}
	if o.listen != "" {
		a = append(a, "--listen", o.listen)
	}
	if o.mdnsOff {
		a = append(a, "--no-mdns")
	}
	if o.dhtOff {
		a = append(a, "--no-dht")
	}
	if o.uploadRateSet {
		a = append(a, "--upload-rate", fmt.Sprint(o.uploadRate))
	}
	if o.downloadRateSet {
		a = append(a, "--download-rate", fmt.Sprint(o.downloadRate))
	}
	for _, set := range []struct {
		flag   string
		values []string
	}{{"--peer", o.peers}, {"--bootstrap", o.bootstrap}, {"--relay-peer", o.relays}, {"--discovery-server", o.servers}, {"--ignore", o.ignores}} {
		for _, v := range set.values {
			a = append(a, set.flag, v)
		}
	}
	return a
}

func applyBandwidthPreferences(cfg *config.Config, o options) {
	// An omitted flag preserves a stored limit; explicitly supplying zero
	// clears it, including when the foreground launcher spawns a worker.
	if o.uploadRateSet {
		cfg.UploadBytesPerSecond = o.uploadRate
	}
	if o.downloadRateSet {
		cfg.DownloadBytesPerSecond = o.downloadRate
	}
}
func launch(o options) error {
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(binary, workerArgs(o)...)
	daemon.Detach(cmd)
	cmd.Stdin = strings.NewReader(o.key)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err = config.EnsureStateDir(o.folder); err != nil {
		return err
	}
	nativeLog, err := openLog(o.folder)
	if err != nil {
		return err
	}
	defer nativeLog.Close()
	cmd.Stderr = nativeLog
	if err = cmd.Start(); err != nil {
		return err
	}
	ch := make(chan ready, 1)
	go func() {
		var r ready
		err := json.NewDecoder(io.LimitReader(out, 65536)).Decode(&r)
		if err != nil {
			r.Error = "background worker failed before readiness"
		}
		ch <- r
	}()
	select {
	case r := <-ch:
		if r.Error != "" {
			cmd.Process.Kill()
			cmd.Wait()
			return errors.New(r.Error)
		}
		if r.Key == "" || r.Status.PID == 0 {
			cmd.Process.Kill()
			cmd.Wait()
			return errors.New("invalid worker readiness confirmation")
		}
		printStart(r.Key, r.Status, "background")
		out.Close()
		return cmd.Process.Release()
	case <-time.After(45 * time.Second):
		cmd.Process.Kill()
		cmd.Wait()
		return errors.New("background startup timed out")
	}
}
func start(o options) error {
	lock, err := daemon.Lock(o.folder)
	if err != nil {
		return err
	}
	defer lock.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if o.worker {
		guard, err := status.StartNativeLogGuard(ctx, filepath.Join(o.folder, ".HappySync", "logs", "worker.log"), 10<<20, time.Second)
		if err != nil {
			return err
		}
		defer guard.Close()
		go func() {
			if err, ok := <-guard.Failures(); ok && err != nil {
				fmt.Fprintln(os.Stderr, "HappySync: native diagnostic log retention failed:", status.Redact(err.Error()))
				cancel()
			}
		}()
	}
	cfg, err := config.Init(o.folder, o.key)
	if err != nil {
		return err
	}
	if len(o.peers) > 0 {
		cfg.Peers = o.peers
	}
	if len(o.bootstrap) > 0 {
		cfg.Bootstrap = o.bootstrap
	}
	if len(o.relays) > 0 {
		cfg.Relays = o.relays
	}
	if len(o.servers) > 0 {
		cfg.DiscoveryServers = o.servers
	}
	if len(o.ignores) > 0 {
		cfg.Ignore = o.ignores
	}
	if o.mdnsOff {
		cfg.DisableMDNS = true
	}
	if o.dhtOff {
		cfg.DisableDHT = true
	}
	if o.listen != "" {
		cfg.Listen = []string{o.listen}
	}
	applyBandwidthPreferences(cfg, o)
	if err = cfg.Save(); err != nil {
		return err
	}
	netCfg := network.Config{Peers: cfg.Peers, Bootstrap: cfg.Bootstrap, Relays: cfg.Relays, DiscoveryServers: cfg.DiscoveryServers, DisableMDNS: cfg.DisableMDNS, DisableDHT: cfg.DisableDHT, Listen: cfg.Listen, UploadBytesPerSecond: cfg.UploadBytesPerSecond, DownloadBytesPerSecond: cfg.DownloadBytesPerSecond}
	var logWriter io.Writer = status.RedactingWriter{Writer: os.Stderr}
	if o.worker {
		rotating, err := status.NewRotatingWriter(filepath.Join(o.folder, ".HappySync", "logs", "operations.log"), 10<<20, 2)
		if err != nil {
			return err
		}
		defer rotating.Close()
		logWriter = rotating
	}
	var previous string
	lastLog := time.Time{}
	logStatus := func(s engine.Status) {
		signature := fmt.Sprintf("%s:%d:%v:%d", s.State, s.Peers.Connected, s.Errors, len(s.Warnings))
		if signature == previous && time.Since(lastLog) < 30*time.Second {
			return
		}
		previous = signature
		lastLog = time.Now()
		json.NewEncoder(logWriter).Encode(map[string]any{"time": lastLog.UTC(), "event": "sync_status", "state": s.State, "connected_peers": s.Peers.Connected, "pending_files": s.PendingFiles, "errors": s.Errors, "warnings": s.Warnings})
	}
	binary, _ := os.Executable()
	if resolved, err := filepath.EvalSymlinks(binary); err == nil {
		binary = resolved
	}
	e, err := engine.New(ctx, cfg, engine.Options{Log: logStatus, Network: netCfg, Workers: o.workers, StagingLimit: o.staging, ScanInterval: o.scan, Ignore: nil, Executable: binary})
	if err != nil {
		return err
	}
	defer e.Close()
	c, err := daemon.Listen(o.folder, func(command string) any {
		if command == "stop" {
			cancel()
			return map[string]string{"state": "stopping"}
		}
		return e.Status()
	})
	if err != nil {
		cancel()
		e.Wait()
		return err
	}
	defer c.Close()
	if o.worker {
		json.NewEncoder(os.Stdout).Encode(ready{Key: cfg.Key, Status: e.Status()})
		os.Stdout.Close()
	} else {
		printStart(cfg.Key, e.Status(), "foreground")
	}
	<-ctx.Done()
	e.Wait()
	return nil
}
func openLog(root string) (*os.File, error) {
	return status.OpenNativeLog(filepath.Join(root, ".HappySync", "logs", "worker.log"), 10<<20)
}
func printStart(key string, s engine.Status, mode string) {
	fmt.Printf("HappySync started\nFolder: %s\nSync ID: %s\nShare key: %s\nSecret: anyone with this key can access and modify synchronized files\nPeer ID: %s\nMode: %s\nStatus: HappySync status --folder %q\n", s.Folder, s.SyncID, key, s.PeerID, mode, s.Folder)
	for _, a := range s.Addresses {
		fmt.Println("Address:", a)
	}
}
func control(command string, o options) error {
	if command == "stop" {
		var out map[string]string
		if err := daemon.Request(o.folder, "stop", &out); err != nil {
			return err
		}
		fmt.Println("HappySync stopping")
		return nil
	}
	var s engine.Status
	if err := daemon.Request(o.folder, "status", &s); err != nil {
		s = engine.Status{Version: 1, Folder: o.folder, State: "stopped", Warnings: []string{}, Errors: []string{}}
		if c, e := config.Load(o.folder); e == nil {
			s.SyncID = hex.EncodeToString(c.SyncID[:6])
		}
	}
	if o.json {
		return json.NewEncoder(os.Stdout).Encode(s)
	}
	fmt.Printf("Folder: %s\nSync ID: %s\nState: %s\nPeers: %d connected (%d direct, %d relayed); %d discovered; %d authenticated\nLocal files: %d   Pending files: %d\nDownload: %.1f MiB/s   Upload: %.1f MiB/s\nTransferred: %d bytes received, %d sent\nOutstanding blocks: %d   Conflicts: %d   Warnings: %d\nDiscovery: mDNS %s; DHT %s; custom %s; relay %s\n", s.Folder, s.SyncID, s.State, s.Peers.Connected, s.Peers.Direct, s.Peers.Relayed, s.Peers.Discovered, s.Peers.Authenticated, s.LocalFiles, s.PendingFiles, s.Download/(1<<20), s.Upload/(1<<20), s.Received, s.Sent, s.OutstandingBlocks, s.Conflicts, len(s.Warnings), s.Peers.MDNS, s.Peers.DHT, s.Peers.Discovery, s.Peers.Relay)
	for _, e := range s.Errors {
		fmt.Println("Error:", e)
	}
	for _, w := range s.Warnings {
		fmt.Println("Warning:", w)
	}
	return nil
}
func serve(o options) error {
	if !o.discovery && !o.relay && !o.serverDHT {
		return errors.New("serve requires --discovery, --relay or --dht-server")
	}
	if o.relayTotalRate < 1 || o.relayPeerRate < 1 || o.relayDataLimit < 1 || o.relayDuration < time.Second || o.maxReservations < 1 || o.maxCircuits < 1 || o.maxRelayStreams < 1 || o.discoveryMaxGroups < 1 || o.discoveryMaxRegistrations < 1 || o.discoveryMaxPerPeer < 1 {
		return errors.New("server resource limits must be positive")
	}
	if o.listen == "" {
		o.listen = "0.0.0.0:4001"
	}
	host, port, err := net.SplitHostPort(o.listen)
	if err != nil {
		return errors.New("server --listen must be host:port")
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return errors.New("server listen host must be an IP address")
	}
	family := "ip4"
	if ip.To4() == nil {
		family = "ip6"
	}
	base := "/" + family + "/" + ip.String()
	listen := []string{base + "/tcp/" + port, base + "/udp/" + port + "/quic-v1"}
	dir, err := filepath.Abs(o.stateDir)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if err = config.ProtectPrivatePath(dir, true); err != nil {
		return err
	}
	identityPath := filepath.Join(dir, "identity.key")
	var identity crypto.PrivKey
	if info, e := os.Lstat(identityPath); e == nil && !info.Mode().IsRegular() {
		return errors.New("server identity must be a regular private file")
	}
	if b, e := os.ReadFile(identityPath); e == nil {
		identity, err = crypto.UnmarshalPrivateKey(b)
	} else if os.IsNotExist(e) {
		identity, _, err = crypto.GenerateEd25519Key(rand.Reader)
		if err == nil {
			b, err = crypto.MarshalPrivateKey(identity)
			if err == nil {
				f, e := os.OpenFile(identityPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
				if e != nil {
					return e
				}
				_, err = f.Write(b)
				if err == nil {
					err = f.Sync()
				}
				f.Close()
			}
		}
	} else {
		return e
	}
	if err != nil {
		return errors.New("cannot initialize server identity")
	}
	if err = config.ProtectPrivatePath(identityPath, false); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	server, err := network.Serve(ctx, network.ServerConfig{Identity: identity, Listen: listen, Advertise: o.advertise, Discovery: o.discovery, Relay: o.relay, DHT: o.serverDHT, Bootstrap: o.bootstrap, AllowPeers: o.allowPeers, RelayTotalBytesPerSecond: o.relayTotalRate, RelayPeerBytesPerSecond: o.relayPeerRate, RelayDataLimit: o.relayDataLimit, RelayDuration: o.relayDuration, MaxReservations: o.maxReservations, MaxCircuits: o.maxCircuits, MaxRelayStreams: o.maxRelayStreams, MaxDiscoveryGroups: o.discoveryMaxGroups, MaxDiscoveryRegistrations: o.discoveryMaxRegistrations, MaxDiscoveryRegistrationsPerPeer: o.discoveryMaxPerPeer})
	if err != nil {
		return err
	}
	defer server.Close()
	fmt.Println("HappySync server started\nPeer ID:", server.PeerID())
	for _, a := range server.Addresses() {
		fmt.Println("Address:", a)
	}
	metrics := time.NewTicker(30 * time.Second)
	defer metrics.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-metrics.C:
			json.NewEncoder(os.Stderr).Encode(map[string]any{"event": "server_metrics", "metrics": server.Stats()})
		}
	}
}
