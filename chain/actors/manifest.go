package actors

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	multihash "github.com/multiformats/go-multihash/core"
	"golang.org/x/xerrors"

	actorstypes "github.com/filecoin-project/go-state-types/actors"
	"github.com/filecoin-project/go-state-types/manifest"

	"github.com/filecoin-project/lotus/chain/actors/adt"
)

// XXX: FIXME FILL
var manifestCids = make(map[string]cid.Cid)
var manifests = make(map[string]map[string]cid.Cid)
var actorMeta = make(map[cid.Cid]actorEntry)

var (
	manifestMx sync.RWMutex
)

type actorEntry struct {
	name    string
	tag     string
	version actorstypes.Version
}

func legacyManifests() (map[string]map[string]cid.Cid, map[cid.Cid]actorEntry) {
	manifests := make(map[string]map[string]cid.Cid)
	metadata := make(map[cid.Cid]actorEntry)
	builder := cid.V1Builder{Codec: cid.Raw, MhType: multihash.IDENTITY}
	for i, v := range []actorstypes.Version{0, 2, 3, 4, 5, 6, 7} {
		// TODO: use the correct tags? E.g., 0.9.1?
		tag := fmt.Sprintf("v%d.0.0", v)
		m := make(map[string]cid.Cid)
		for _, key := range manifest.GetBuiltinActorsKeys(v) {
			// We use i+1 because the first version is 0, but the version used in the
			// name is 1.
			cid, err := builder.Sum([]byte(fmt.Sprintf("fil/%d/%s", i+1, key)))
			if err != nil {
				panic(err)
			}
			m[key] = cid
			metadata[cid] = actorEntry{
				name:    key,
				tag:     tag,
				version: v,
			}
		}

		manifests[tag] = m
	}
	return manifests, metadata
}

// ClearManifests clears all known manifests. This is usually used in tests that need to switch networks.
func ClearManifests() {
	manifestMx.Lock()
	defer manifestMx.Unlock()

	manifestCids = make(map[string]cid.Cid)
	manifests, actorMeta = legacyManifests()
}

// RegisterManifest registers an actors manifest with lotus.
func RegisterManifest(tag string, av actorstypes.Version, manifestCid cid.Cid, entries map[string]cid.Cid) {
	manifestMx.Lock()
	defer manifestMx.Unlock()

	manifestCids[tag] = manifestCid
	manifests[tag] = entries

	for name, c := range entries {
		actorMeta[c] = actorEntry{name: name, tag: tag, version: av}
	}
}

// GetManifest gets a loaded manifest.
func GetManifest(tag string) (cid.Cid, bool) {
	manifestMx.RLock()
	defer manifestMx.RUnlock()

	c, ok := manifestCids[tag]
	return c, ok
}

// ReadManifest reads a manifest from a blockstore. It does not "add" it.
func ReadManifest(ctx context.Context, store cbor.IpldStore, mfCid cid.Cid) (map[string]cid.Cid, error) {
	adtStore := adt.WrapStore(ctx, store)

	var mf manifest.Manifest
	if err := adtStore.Get(ctx, mfCid, &mf); err != nil {
		return nil, xerrors.Errorf("error reading manifest (cid: %s): %w", mfCid, err)
	}

	if err := mf.Load(ctx, adtStore); err != nil {
		return nil, xerrors.Errorf("error loading manifest (cid: %s): %w", mfCid, err)
	}

	var manifestData manifest.ManifestData
	if err := store.Get(ctx, mf.Data, &manifestData); err != nil {
		return nil, xerrors.Errorf("error loading manifest data: %w", err)
	}

	metadata := make(map[string]cid.Cid)
	for _, entry := range manifestData.Entries {
		metadata[entry.Name] = entry.Code
	}

	return metadata, nil
}

// GetActorCodeIDs looks up all builtin actor's code CIDs by actor release tag.
func GetActorCodeIDs(tag string) (map[string]cid.Cid, bool) {
	manifestMx.RLock()
	defer manifestMx.RUnlock()

	cids, ok := manifests[tag]
	return cids, ok
}

// GetActorCodeID looks up a builtin actor's code CID by actor tag and canonical actor name.
func GetActorCodeID(tag string, name string) (cid.Cid, bool) {
	cids, ok := GetActorCodeIDs(tag)
	if !ok {
		return cid.Undef, false
	}
	cid, ok := cids[name]
	return cid, ok
}

// Given a Manifest CID, get the manifest from the store and Load data into its entries
func LoadManifest(ctx context.Context, mfCid cid.Cid, adtStore adt.Store) (*manifest.Manifest, error) {
	var mf manifest.Manifest

	if err := adtStore.Get(ctx, mfCid, &mf); err != nil {
		return nil, xerrors.Errorf("error reading manifest: %w", err)
	}

	if err := mf.Load(ctx, adtStore); err != nil {
		return nil, xerrors.Errorf("error loading manifest entries data: %w", err)
	}

	return &mf, nil
}

// GetActorMetaByCode returns the:
//
// - Actor name
// - Actor "git tag" (code version)
// - Actor Version (API version)
// - A boolean indicating whether or not the actor was found.
func GetActorMetaByCode(c cid.Cid) (string, string, actorstypes.Version, bool) {
	manifestMx.RLock()
	defer manifestMx.RUnlock()

	entry, ok := actorMeta[c]
	if !ok {
		return "", "", -1, false
	}

	return entry.name, entry.tag, entry.version, true
}

func CanonicalName(name string) string {
	idx := strings.LastIndex(name, "/")
	if idx >= 0 {
		return name[idx+1:]
	}

	return name
}
