// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package dist_cache

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-storage-fuse/v2/common/config"
	"github.com/Azure/azure-storage-fuse/v2/common/log"
	"github.com/Azure/azure-storage-fuse/v2/internal"

	dcache "github.com/Azure/azure-storage-fuse/v2/internal/dist_cache_client"
	"golang.org/x/sync/errgroup"
)

const compName = "dist_cache"

// maxParallelChunkOps limits the number of concurrent chunk-level recovery
// operations (Azure fetches and cache polls) during CopyToFile.
const maxParallelChunkOps = 8

// maxPendingL2Uploads limits the number of concurrent L2 cache uploads when
// flushing pending chunks at commit time.
const maxPendingL2Uploads = 8

// pendingWriteTTL is the maximum time pending chunks are held before being
// evicted. This handles abandoned writes (e.g., process crash before commit,
// lazy-write with long-lived handles).
const pendingWriteTTL = 5 * time.Minute

// pendingCleanupInterval is how often the background goroutine scans for
// expired pending entries.
const pendingCleanupInterval = 30 * time.Second

// DistCacheOptions holds configuration for the distributed cache component.
type DistCacheOptions struct {
	// Discovery (preferred — auto-detects servers)
	DiscoveryURL        string `config:"discovery-url"        yaml:"discovery-url,omitempty"`
	DiscoveryRefreshSec int    `config:"discovery-refresh-sec" yaml:"discovery-refresh-sec,omitempty"`

	// Kubernetes DNS discovery
	K8sService   string `config:"k8s-service"   yaml:"k8s-service,omitempty"`
	K8sNamespace string `config:"k8s-namespace" yaml:"k8s-namespace,omitempty"`

	// Static fallback
	ServerList string `config:"server-list" yaml:"server-list,omitempty"`

	// Common options
	Port            int    `config:"port"              yaml:"port,omitempty"`
	TTLSeconds      uint32 `config:"ttl-seconds"       yaml:"ttl-seconds,omitempty"`
	MaxFileSizeMB   int    `config:"max-file-size-mb"  yaml:"max-file-size-mb,omitempty"`
	AuthAccountName string `config:"auth-account-name" yaml:"auth-account-name,omitempty"`
	AuthAccountKey  string `config:"auth-account-key"  yaml:"auth-account-key,omitempty"`
	HashType        string `config:"hash-type"         yaml:"hash-type,omitempty"`
	BypassOnError   bool   `config:"bypass-on-error"   yaml:"bypass-on-error,omitempty"`
	CachePrefix     string `config:"cache-prefix"      yaml:"cache-prefix,omitempty"`
	MaxConnsPerSvr  int    `config:"max-conns-per-server" yaml:"max-conns-per-server,omitempty"`

	// Chunk size for distributed cache operations. When block_cache is present,
	// this is overridden by block_cache.block-size-mb to keep alignment consistent.
	// When used with file_cache (no block_cache), this is the primary chunk size config.
	ChunkSizeMB float64 `config:"chunk-size-mb" yaml:"chunk-size-mb,omitempty"`
}

// pendingChunk holds a staged chunk's data until the file is committed.
type pendingChunk struct {
	offset int64
	data   []byte
}

// pendingFile tracks buffered chunks for a single file along with metadata
// for size-cap and TTL-based eviction.
type pendingFile struct {
	chunks       []pendingChunk
	totalSize    int64     // sum of len(chunk.data) for all chunks
	lastActivity time.Time // updated on each StageData; used for TTL eviction
}

// DistCache is the blobfuse component that sits between the local cache and azstorage,
// providing a shared distributed cache layer across nodes.
type DistCache struct {
	internal.BaseComponent
	conf   DistCacheOptions
	client dcacheClient

	chunkSize     int64
	bypassOnError bool

	// dirtyFiles tracks recently invalidated files to avoid serving stale data.
	// After a delete/truncate/rename, the file is added here. Reads for files
	// in this set bypass dist_cache until the TTL expires, giving the server
	// time to process the async group-based deletion.
	dirtyMu    sync.Mutex
	dirtyFiles map[string]time.Time

	// pendingMu protects pendingWrites. Chunks are buffered here during
	// StageData and flushed to L2 only after CommitData succeeds, preventing
	// other nodes from reading partially-written data.
	pendingMu     sync.Mutex
	pendingWrites map[string]*pendingFile

	// flushMu protects flushCancel. When a new commit or invalidation arrives
	// for a file, any in-flight flush goroutine for that file is cancelled to
	// prevent it from uploading stale data after a DeleteGroup.
	flushMu     sync.Mutex
	flushCancel map[string]context.CancelFunc

	// versionMu protects fileVersions. Each file has a monotonically increasing
	// version number used to construct versioned group IDs. This ensures that
	// an async server-side DeleteGroup for version N cannot affect chunks
	// uploaded under version N+1.
	versionMu    sync.Mutex
	fileVersions map[string]uint64

	// stopCleanup signals the background pending-writes cleanup goroutine to exit.
	stopCleanup chan struct{}
}

const dirtyTTL = 10 * time.Second

// dcacheClient abstracts the distributed cache client for testing.
type dcacheClient interface {
	Upload(ctx context.Context, filename string, data io.Reader, size int64, opts ...dcache.UploadOption) error
	DownloadWithSizePartial(ctx context.Context, filename string, fileSize int64, w io.WriterAt, opts ...dcache.DownloadOption) ([]dcache.ChunkError, error)
	DownloadChunk(ctx context.Context, filename string, offset int64, buf []byte, opts ...dcache.DownloadOption) (int, error)
	UploadChunk(ctx context.Context, filename string, offset int64, data []byte, opts ...dcache.UploadOption) error
	Delete(ctx context.Context, filename string, fileSize int64) error
	DeleteGroup(ctx context.Context, groupID []byte) error
	GetChunkGroupID(ctx context.Context, filename string) ([]byte, error)
	GetAttr(ctx context.Context, filename string) (*dcache.FileAttr, error)
	PutAttr(ctx context.Context, attrs []dcache.FileAttrEntry) error
	Close() error
}

// Verify interface compliance.
var _ internal.Component = &DistCache{}

func NewDistCacheComponent() internal.Component {
	comp := &DistCache{
		dirtyFiles:    make(map[string]time.Time),
		pendingWrites: make(map[string]*pendingFile),
		flushCancel:   make(map[string]context.CancelFunc),
		fileVersions:  make(map[string]uint64),
		stopCleanup:   make(chan struct{}),
	}
	comp.SetName(compName)
	return comp
}

func init() {
	internal.AddComponent(compName, NewDistCacheComponent)

	discoveryFlag := config.AddStringFlag("dist-cache-discovery-url", "",
		"distributed cache discovery endpoint (recommended)")
	config.BindPFlag(compName+".discovery-url", discoveryFlag)

	serverListFlag := config.AddStringFlag("dist-cache-server-list", "",
		"comma-separated list of distributed cache server addresses (fallback)")
	config.BindPFlag(compName+".server-list", serverListFlag)

	// Support DIST_CACHE_SERVER_LIST env var
	config.BindEnv(compName+".server-list", "DIST_CACHE_SERVER_LIST")
}

func (dc *DistCache) Configure(isParent bool) error {
	log.Trace("DistCache::Configure")

	conf := DistCacheOptions{}
	err := config.UnmarshalKey(compName, &conf)
	if err != nil {
		log.Err("DistCache: config error [invalid config attributes]")
		return fmt.Errorf("dist_cache: config error: %w", err)
	}

	// Validate that at least one server discovery method is configured
	if conf.DiscoveryURL == "" && conf.K8sService == "" && conf.ServerList == "" {
		if os.Getenv("DIST_CACHE_SERVER_LIST") == "" {
			return fmt.Errorf("dist_cache: no server discovery configured (set discovery-url, k8s-service, server-list, or DIST_CACHE_SERVER_LIST)")
		}
	}

	dc.conf = conf
	dc.bypassOnError = conf.BypassOnError

	// Resolve chunk size: block_cache.block-size-mb > stream.block-size-mb > dist_cache.chunk-size-mb > default
	const defaultBlockSizeMB = 16
	var blockSizeMB float64 = defaultBlockSizeMB
	if config.IsSet("block_cache.block-size-mb") {
		err = config.UnmarshalKey("block_cache.block-size-mb", &blockSizeMB)
		if err != nil {
			log.Warn("DistCache::Configure : Failed to read block-size-mb, using default %d MB", defaultBlockSizeMB)
			blockSizeMB = defaultBlockSizeMB
		}
	} else if config.IsSet("stream.block-size-mb") {
		err = config.UnmarshalKey("stream.block-size-mb", &blockSizeMB)
		if err != nil {
			blockSizeMB = defaultBlockSizeMB
		}
	} else if conf.ChunkSizeMB > 0 {
		blockSizeMB = conf.ChunkSizeMB
	}
	dc.chunkSize = int64(blockSizeMB * 1024 * 1024)

	log.Info("DistCache::Configure : chunk-size=%d, bypass-on-error=%v",
		dc.chunkSize, dc.bypassOnError)

	return nil
}

func (dc *DistCache) Start(ctx context.Context) error {
	log.Trace("Starting component : %s", dc.Name())

	// Build client options
	opts := []dcache.Option{
		dcache.WithChunkSize(dc.chunkSize),
	}

	if dc.conf.DiscoveryURL != "" {
		opts = append(opts, dcache.WithDiscoveryURL(dc.conf.DiscoveryURL))
	}
	if dc.conf.K8sService != "" && dc.conf.K8sNamespace != "" {
		opts = append(opts, dcache.WithK8sDiscovery(dc.conf.K8sService, dc.conf.K8sNamespace))
	}
	if dc.conf.ServerList != "" {
		servers := strings.Split(dc.conf.ServerList, ",")
		for i := range servers {
			servers[i] = strings.TrimSpace(servers[i])
		}
		opts = append(opts, dcache.WithServerList(servers))
	}
	if dc.conf.Port > 0 {
		opts = append(opts, dcache.WithPort(dc.conf.Port))
	}
	if dc.conf.AuthAccountName != "" {
		opts = append(opts, dcache.WithAuth(dc.conf.AuthAccountName, dc.conf.AuthAccountKey))
	}
	if dc.conf.CachePrefix != "" {
		opts = append(opts, dcache.WithCachePrefix(dc.conf.CachePrefix))
	}
	if dc.conf.MaxConnsPerSvr > 0 {
		opts = append(opts, dcache.WithMaxConnsPerServer(dc.conf.MaxConnsPerSvr))
	}
	if dc.conf.DiscoveryRefreshSec > 0 {
		opts = append(opts, dcache.WithDiscoveryRefresh(
			time.Duration(dc.conf.DiscoveryRefreshSec)*time.Second))
	}

	client, err := dcache.New(opts...)
	if err != nil {
		if dc.bypassOnError {
			log.Warn("DistCache::Start : Failed to connect to distributed cache, bypassing: %v", err)
			return nil
		}
		return fmt.Errorf("dist_cache: failed to start: %w", err)
	}

	dc.client = client
	log.Info("DistCache::Start : connected to distributed cache cluster")

	// Start background goroutine to evict stale pending writes
	go dc.pendingCleanupLoop()

	return nil
}

func (dc *DistCache) Stop() error {
	log.Trace("Stopping component : %s", dc.Name())
	close(dc.stopCleanup)
	if dc.client != nil {
		return dc.client.Close()
	}
	return nil
}

func (dc *DistCache) Priority() internal.ComponentPriority {
	return internal.EComponentPriority.LevelMid()
}

// --- Read path (file_cache) ---

func (dc *DistCache) CopyToFile(options internal.CopyToFileOptions) error {
	if dc.client == nil {
		return dc.NextComponent().CopyToFile(options)
	}

	// Skip dist_cache for recently invalidated files to avoid stale data
	if dc.isDirty(options.Name) {
		log.Debug("DistCache::CopyToFile : dirty, bypassing %s", options.Name)
		return dc.NextComponent().CopyToFile(options)
	}

	ctx := context.Background()

	// Try distributed cache with lock-on-miss enabled, collecting per-chunk misses
	chunkErrors, err := dc.client.DownloadWithSizePartial(ctx, options.Name, options.Count, options.File,
		dcache.WithLock(true))
	if err != nil {
		if dc.bypassOnError {
			log.Warn("DistCache::CopyToFile : error, bypassing: %v", err)
			return dc.NextComponent().CopyToFile(options)
		}
		return err
	}

	if len(chunkErrors) == 0 {
		log.Debug("DistCache::CopyToFile : L2 hit %s", options.Name)
		return nil
	}

	// Handle per-chunk cache misses in parallel
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxParallelChunkOps)

	for _, ce := range chunkErrors {
		ce := ce
		switch ce.Err {
		case dcache.ErrNotFoundGotLock:
			g.Go(func() error {
				log.Debug("DistCache::CopyToFile : L2 chunk miss (got lock) %s offset=%d", options.Name, ce.Offset)
				return dc.fetchChunkFromRemote(gctx, options, ce.Offset, ce.Size, true)
			})

		case dcache.ErrNotFoundAlreadyLocked:
			g.Go(func() error {
				log.Debug("DistCache::CopyToFile : L2 chunk miss (locked) %s offset=%d, polling", options.Name, ce.Offset)
				if err := dc.pollUntilChunkCached(gctx, options, ce.Offset, ce.Size); err != nil {
					log.Debug("DistCache::CopyToFile : chunk poll timeout %s offset=%d, falling through", options.Name, ce.Offset)
					return dc.fetchChunkFromRemote(gctx, options, ce.Offset, ce.Size, false)
				}
				return nil
			})

		default:
			g.Go(func() error {
				log.Debug("DistCache::CopyToFile : L2 chunk miss %s offset=%d", options.Name, ce.Offset)
				return dc.fetchChunkFromRemote(gctx, options, ce.Offset, ce.Size, false)
			})
		}
	}

	return g.Wait()
}

// --- Write path (file_cache) ---

func (dc *DistCache) CopyFromFile(options internal.CopyFromFileOptions) error {
	// Write-through to azstorage first (source of truth)
	err := dc.NextComponent().CopyFromFile(options)
	if err != nil {
		return err
	}

	if dc.client == nil {
		return nil
	}

	// Cancel any in-flight flush/populate from a previous write
	dc.cancelFlush(options.Name)

	// Mark dirty so other nodes bypass stale L2 data during the populate window
	dc.markDirty(options.Name)

	// Invalidate old L2 entry. Query the server to discover the actual group
	// ID (handles crash/restart where local version was lost), then bump so
	// the new upload uses a different group ID.
	if err := dc.client.DeleteGroup(context.Background(), dc.resolveServerGroupID(options.Name)); err != nil {
		log.Warn("DistCache::CopyFromFile : L2 invalidation failed for %s: %v", options.Name, err)
	}
	newVer := dc.bumpVersion(options.Name)

	// Populate distributed cache (best-effort, async)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	dc.flushMu.Lock()
	dc.flushCancel[options.Name] = cancel
	dc.flushMu.Unlock()
	go dc.populateCache(ctx, options.Name, options.File.Name(), newVer)
	return nil
}

// --- Read path (block_cache) ---

// resolveReadPath returns the file path for a ReadInBuffer call. block_cache
// sets Handle but not Path; file_cache/azstorage may set Path directly.
func resolveReadPath(options *internal.ReadInBufferOptions) string {
	if options.Path != "" {
		return options.Path
	}
	if options.Handle != nil {
		return options.Handle.Path
	}
	return ""
}

func (dc *DistCache) ReadInBuffer(options *internal.ReadInBufferOptions) (int, error) {
	if dc.client == nil {
		return dc.NextComponent().ReadInBuffer(options)
	}

	name := resolveReadPath(options)

	// Skip dist_cache for recently invalidated files to avoid stale data
	if dc.isDirty(name) {
		log.Debug("DistCache::ReadInBuffer : dirty, bypassing %s", name)
		return dc.NextComponent().ReadInBuffer(options)
	}

	ctx := context.Background()

	n, err := dc.client.DownloadChunk(ctx, name, options.Offset, options.Data,
		dcache.WithLock(true))
	if err == nil && n > 0 {
		log.Debug("DistCache::ReadInBuffer : L2 hit %s offset=%d", name, options.Offset)
		return n, nil
	}
	if err == nil && n == 0 {
		// Zero-byte hit means corrupt/empty cache entry — treat as miss
		log.Warn("DistCache::ReadInBuffer : L2 zero-byte hit %s offset=%d, falling through to storage", name, options.Offset)
		n, err = dc.NextComponent().ReadInBuffer(options)
		if err != nil {
			return n, err
		}
		if n > 0 {
			dataCopy := make([]byte, n)
			copy(dataCopy, options.Data[:n])
			go dc.uploadChunkAsync(name, options.Offset, dataCopy)
		}
		return n, nil
	}

	if err == dcache.ErrNotFoundGotLock {
		// We own this chunk's miss — download from Azure and populate cache
		log.Debug("DistCache::ReadInBuffer : L2 miss (got lock) %s offset=%d", name, options.Offset)
		n, err = dc.NextComponent().ReadInBuffer(options)
		if err != nil {
			return n, err
		}
		dataCopy := make([]byte, n)
		copy(dataCopy, options.Data[:n])
		go dc.uploadChunkAsync(name, options.Offset, dataCopy)
		return n, nil
	}

	if err == dcache.ErrNotFoundAlreadyLocked {
		// Another node is fetching this chunk — poll until cached
		log.Debug("DistCache::ReadInBuffer : L2 miss (locked) %s offset=%d, polling", name, options.Offset)
		n, pollErr := dc.pollChunkIntoBuffer(ctx, name, options.Offset, options.Data)
		if pollErr == nil {
			return n, nil
		}
		// Poll timed out — fall through to Azure
		log.Debug("DistCache::ReadInBuffer : chunk poll timeout %s offset=%d, falling through", name, options.Offset)
		n, err = dc.NextComponent().ReadInBuffer(options)
		if err != nil {
			return n, err
		}
		dataCopy := make([]byte, n)
		copy(dataCopy, options.Data[:n])
		go dc.uploadChunkAsync(name, options.Offset, dataCopy)
		return n, nil
	}

	if err == dcache.ErrNotFound {
		// L2 miss — read from Azure
		log.Debug("DistCache::ReadInBuffer : L2 miss %s offset=%d", name, options.Offset)
		n, err = dc.NextComponent().ReadInBuffer(options)
		if err != nil {
			return n, err
		}
		dataCopy := make([]byte, n)
		copy(dataCopy, options.Data[:n])
		go dc.uploadChunkAsync(name, options.Offset, dataCopy)
		return n, nil
	}

	if dc.bypassOnError {
		log.Warn("DistCache::ReadInBuffer : error, bypassing: %v", err)
		return dc.NextComponent().ReadInBuffer(options)
	}
	return 0, err
}

// --- Write path (block_cache) ---

func (dc *DistCache) StageData(options internal.StageDataOptions) error {
	// Write-through to azstorage first
	err := dc.NextComponent().StageData(options)
	if err != nil {
		return err
	}

	if dc.client == nil {
		return nil
	}

	dataLen := int64(len(options.Data))
	maxSize := int64(dc.conf.MaxFileSizeMB) * 1024 * 1024

	dc.pendingMu.Lock()
	pf := dc.pendingWrites[options.Name]

	// Size cap: if buffering this chunk would exceed MaxFileSizeMB, drop all
	// pending data for this file. L2 will be warmed via the read path instead.
	// No need to invalidate L2 here — the committed state hasn't changed, so
	// existing L2 data is still valid. CommitData will handle invalidation if
	// and when the write is actually committed.
	if maxSize > 0 && pf != nil && pf.totalSize+dataLen > maxSize {
		log.Debug("DistCache::StageData : pending size would exceed %dMB for %s, skipping L2 write-warming",
			dc.conf.MaxFileSizeMB, options.Name)
		delete(dc.pendingWrites, options.Name)
		dc.pendingMu.Unlock()
		return nil
	}

	// Buffer the chunk for deferred L2 population at commit time.
	dataCopy := make([]byte, dataLen)
	copy(dataCopy, options.Data)

	if pf == nil {
		pf = &pendingFile{}
		dc.pendingWrites[options.Name] = pf
	}
	pf.chunks = append(pf.chunks, pendingChunk{
		offset: int64(options.Offset),
		data:   dataCopy,
	})
	pf.totalSize += dataLen
	pf.lastActivity = time.Now()
	dc.pendingMu.Unlock()

	return nil
}

func (dc *DistCache) CommitData(options internal.CommitDataOptions) error {
	// Forward to azstorage first — commit is the source-of-truth operation
	err := dc.NextComponent().CommitData(options)
	if err != nil {
		return err
	}

	if dc.client == nil {
		return nil
	}

	// Cancel any in-flight flush from a previous commit. This prevents a racing
	// goroutine from uploading stale chunks after our DeleteGroup below.
	dc.cancelFlush(options.Name)

	// Invalidate old L2 entries before flushing new data. CommitData means the
	// file's block list has changed (via O_TRUNC rewrite, append, or partial
	// overwrite), so any previously cached chunks are potentially stale.
	// Query the server for the actual group ID (handles crash/restart), then
	// bump so the flush uses a new group ID safe from async deletion.
	dc.markDirty(options.Name)
	if err := dc.client.DeleteGroup(context.Background(), dc.resolveServerGroupID(options.Name)); err != nil {
		log.Warn("DistCache::CommitData : L2 invalidation failed for %s: %v", options.Name, err)
	}
	newVer := dc.bumpVersion(options.Name)

	// Drain pending chunks and flush to L2 asynchronously now that the
	// file is committed in Azure and safe for other nodes to read.
	dc.pendingMu.Lock()
	pf := dc.pendingWrites[options.Name]
	delete(dc.pendingWrites, options.Name)
	dc.pendingMu.Unlock()

	if pf != nil && len(pf.chunks) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		dc.flushMu.Lock()
		dc.flushCancel[options.Name] = cancel
		dc.flushMu.Unlock()
		go dc.flushPendingToL2(ctx, options.Name, pf.chunks, newVer)
	}
	return nil
}

// --- Invalidation ---

func (dc *DistCache) DeleteFile(options internal.DeleteFileOptions) error {
	if dc.client != nil {
		dc.markDirty(options.Name)
		dc.clearPending(options.Name)
		if err := dc.client.DeleteGroup(context.Background(), dc.resolveServerGroupID(options.Name)); err != nil {
			log.Warn("DistCache::DeleteFile : cache invalidation failed for %s: %v", options.Name, err)
		}
		dc.bumpVersion(options.Name)
	}
	return dc.NextComponent().DeleteFile(options)
}

func (dc *DistCache) RenameFile(options internal.RenameFileOptions) error {
	if dc.client != nil {
		dc.markDirty(options.Src)
		dc.clearPending(options.Src)
		if err := dc.client.DeleteGroup(context.Background(), dc.resolveServerGroupID(options.Src)); err != nil {
			log.Warn("DistCache::RenameFile : cache invalidation failed for %s: %v", options.Src, err)
		}
		dc.bumpVersion(options.Src)
	}
	return dc.NextComponent().RenameFile(options)
}

func (dc *DistCache) TruncateFile(options internal.TruncateFileOptions) error {
	if dc.client != nil {
		dc.markDirty(options.Name)
		dc.clearPending(options.Name)
		if err := dc.client.DeleteGroup(context.Background(), dc.resolveServerGroupID(options.Name)); err != nil {
			log.Warn("DistCache::TruncateFile : cache invalidation failed for %s: %v", options.Name, err)
		}
		dc.bumpVersion(options.Name)
	}
	return dc.NextComponent().TruncateFile(options)
}

// --- Internal helpers ---

// fetchChunkFromRemote downloads a single chunk from Azure via the next component
// and writes it to the file at the correct offset. If populateCache is true,
// the chunk is also uploaded to the distributed cache asynchronously.
func (dc *DistCache) fetchChunkFromRemote(ctx context.Context, options internal.CopyToFileOptions, offset, size int64, populateCache bool) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	buf := make([]byte, size)
	readOpts := &internal.ReadInBufferOptions{
		Path:   options.Name,
		Offset: offset,
		Data:   buf,
		Size:   options.Count,
	}
	n, err := dc.NextComponent().ReadInBuffer(readOpts)
	if err != nil {
		return err
	}
	if _, err := options.File.WriteAt(buf[:n], offset); err != nil {
		return err
	}
	if populateCache {
		dataCopy := make([]byte, n)
		copy(dataCopy, buf[:n])
		go dc.uploadChunkAsync(options.Name, offset, dataCopy)
	}
	return nil
}

// pollUntilChunkCached waits for a single chunk to become available in the
// distributed cache and writes it to the file. Returns nil on success.
func (dc *DistCache) pollUntilChunkCached(ctx context.Context, options internal.CopyToFileOptions, offset, size int64) error {
	buf := make([]byte, size)
	n, err := dc.pollChunkIntoBuffer(ctx, options.Name, offset, buf)
	if err != nil {
		return err
	}
	_, err = options.File.WriteAt(buf[:n], offset)
	return err
}

// pollChunkIntoBuffer waits for a single chunk to become available in the
// distributed cache and copies it into buf. Returns the number of bytes read.
func (dc *DistCache) pollChunkIntoBuffer(ctx context.Context, name string, offset int64, buf []byte) (int, error) {
	const (
		maxPollDuration = 30 * time.Second
		maxBackoff      = 5 * time.Second
	)

	deadline := time.Now().Add(maxPollDuration)
	backoff := 200 * time.Millisecond

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(backoff):
		}

		n, err := dc.client.DownloadChunk(ctx, name, offset, buf)
		if err == nil {
			return n, nil
		}

		if err != dcache.ErrNotFoundAlreadyLocked && err != dcache.ErrNotFound {
			return 0, err
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	return 0, fmt.Errorf("dist_cache: chunk poll timeout for %s offset=%d", name, offset)
}

// markDirty records that a file was recently invalidated. Reads for this file
// will bypass dist_cache until dirtyTTL expires or clearDirty is called.
func (dc *DistCache) markDirty(name string) {
	dc.dirtyMu.Lock()
	dc.dirtyFiles[name] = time.Now()
	dc.dirtyMu.Unlock()
}

// clearDirty removes the dirty flag for a file, allowing reads to use L2 again.
// Called after L2 has been successfully re-populated with fresh data.
func (dc *DistCache) clearDirty(name string) {
	dc.dirtyMu.Lock()
	delete(dc.dirtyFiles, name)
	dc.dirtyMu.Unlock()
}

// isDirty returns true if the file was recently invalidated and should not be
// read from dist_cache.
func (dc *DistCache) isDirty(name string) bool {
	dc.dirtyMu.Lock()
	t, ok := dc.dirtyFiles[name]
	if ok && time.Since(t) > dirtyTTL {
		delete(dc.dirtyFiles, name)
		ok = false
	}
	dc.dirtyMu.Unlock()
	return ok
}

// fileGroupID returns a versioned group ID for a file. All chunks uploaded in
// the same version share this ID. Using a version suffix ensures that an async
// server-side DeleteGroup for an older version cannot affect chunks uploaded
// under a newer version.
func fileGroupID(name string, version uint64) []byte {
	return []byte(fmt.Sprintf("%s\x00v%d", name, version))
}

// clearPending discards any buffered chunks for a file (e.g. on delete/truncate).
func (dc *DistCache) clearPending(name string) {
	dc.pendingMu.Lock()
	delete(dc.pendingWrites, name)
	dc.pendingMu.Unlock()
}

// getVersion returns the current group version for a file.
func (dc *DistCache) getVersion(name string) uint64 {
	dc.versionMu.Lock()
	v := dc.fileVersions[name]
	dc.versionMu.Unlock()
	return v
}

// bumpVersion increments the group version for a file and returns the new version.
// Must be called after DeleteGroup so that subsequent uploads use a new group ID
// that won't be affected by the async server-side deletion.
func (dc *DistCache) bumpVersion(name string) uint64 {
	dc.versionMu.Lock()
	dc.fileVersions[name]++
	v := dc.fileVersions[name]
	dc.versionMu.Unlock()
	return v
}

// resolveServerGroupID queries the server for the actual group ID stored in
// chunk metadata. This handles the case where the local version counter was
// lost (e.g., after a crash/restart) and the server has chunks under a
// different version. Falls back to the local version if the server has no data.
func (dc *DistCache) resolveServerGroupID(name string) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	serverGID, err := dc.client.GetChunkGroupID(ctx, name)
	if err == nil && serverGID != nil {
		return serverGID
	}

	// Server has no data or error — use local version (best effort)
	return fileGroupID(name, dc.getVersion(name))
}

// cancelFlush cancels any in-flight flush goroutine for the given file.
// Must be called before DeleteGroup to prevent a racing flush from re-uploading
// stale data after the group has been deleted.
func (dc *DistCache) cancelFlush(name string) {
	dc.flushMu.Lock()
	if cancel, ok := dc.flushCancel[name]; ok {
		cancel()
		delete(dc.flushCancel, name)
	}
	dc.flushMu.Unlock()
}

// flushPendingToL2 uploads all buffered chunks for a file to the distributed
// cache. Called asynchronously after CommitData succeeds. The version parameter
// is the bumped version to use for the new group ID.
func (dc *DistCache) flushPendingToL2(ctx context.Context, name string, chunks []pendingChunk, version uint64) {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxPendingL2Uploads)

	for i := range chunks {
		chunk := chunks[i]
		g.Go(func() error {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
			}

			gid := fileGroupID(name, version)
			opts := []dcache.UploadOption{
				dcache.WithIgnoreLock(true),
				dcache.WithGroupID(gid),
				dcache.WithMetadata(map[string][]byte{"gid": gid}),
			}
			if dc.conf.TTLSeconds > 0 {
				opts = append(opts, dcache.WithTTL(dc.conf.TTLSeconds))
			}

			if err := dc.client.UploadChunk(gctx, name, chunk.offset, chunk.data, opts...); err != nil {
				log.Warn("DistCache::flushPendingToL2 : upload failed for %s offset=%d: %v", name, chunk.offset, err)
			}
			return nil // best-effort: don't abort other uploads on failure
		})
	}

	_ = g.Wait()
	log.Debug("DistCache::flushPendingToL2 : flushed %d chunks for %s", len(chunks), name)

	// Only clear dirty if we weren't cancelled (a cancellation means a new
	// commit/invalidation is in progress and will manage the dirty state).
	if ctx.Err() == nil {
		dc.clearDirty(name)
	}

	// Clean up the cancel entry
	dc.flushMu.Lock()
	delete(dc.flushCancel, name)
	dc.flushMu.Unlock()
}

// pendingCleanupLoop periodically evicts pending entries that have exceeded
// pendingWriteTTL. This handles abandoned writes where CommitData is never called.
func (dc *DistCache) pendingCleanupLoop() {
	ticker := time.NewTicker(pendingCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-dc.stopCleanup:
			return
		case <-ticker.C:
			dc.evictStalePending()
		}
	}
}

// evictStalePending removes pending entries whose lastActivity exceeds pendingWriteTTL.
func (dc *DistCache) evictStalePending() {
	now := time.Now()
	dc.pendingMu.Lock()
	for name, pf := range dc.pendingWrites {
		if now.Sub(pf.lastActivity) > pendingWriteTTL {
			log.Debug("DistCache::evictStalePending : evicting %d stale chunks for %s (idle %v)",
				len(pf.chunks), name, now.Sub(pf.lastActivity))
			delete(dc.pendingWrites, name)
		}
	}
	dc.pendingMu.Unlock()
}

func (dc *DistCache) populateCache(ctx context.Context, name string, filePath string, version uint64) {
	defer func() {
		dc.flushMu.Lock()
		delete(dc.flushCancel, name)
		dc.flushMu.Unlock()
	}()

	// Re-open the file by path (the original handle may be closed by the caller)
	f, err := os.Open(filePath)
	if err != nil {
		log.Warn("DistCache::populateCache : open failed: %v", err)
		return
	}
	defer f.Close()

	// Get file size
	info, err := f.Stat()
	if err != nil {
		log.Warn("DistCache::populateCache : stat failed: %v", err)
		return
	}

	gid := fileGroupID(name, version)
	opts := []dcache.UploadOption{
		dcache.WithIgnoreLock(true),
		dcache.WithGroupID(gid),
		dcache.WithMetadata(map[string][]byte{"gid": gid}),
	}
	if dc.conf.TTLSeconds > 0 {
		opts = append(opts, dcache.WithTTL(dc.conf.TTLSeconds))
	}

	if err := dc.client.Upload(ctx, name, f, info.Size(), opts...); err != nil {
		log.Warn("DistCache::populateCache : upload failed: %v", err)
		return
	}

	// Only clear dirty if we weren't cancelled
	if ctx.Err() == nil {
		dc.clearDirty(name)
	}
}

func (dc *DistCache) uploadChunkAsync(name string, offset int64, data []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gid := fileGroupID(name, dc.getVersion(name))
	opts := []dcache.UploadOption{
		dcache.WithIgnoreLock(true),
		dcache.WithGroupID(gid),
		dcache.WithMetadata(map[string][]byte{"gid": gid}),
	}
	if dc.conf.TTLSeconds > 0 {
		opts = append(opts, dcache.WithTTL(dc.conf.TTLSeconds))
	}

	if err := dc.client.UploadChunk(ctx, name, offset, data, opts...); err != nil {
		log.Warn("DistCache::uploadChunkAsync : upload failed: %v", err)
	}
}
