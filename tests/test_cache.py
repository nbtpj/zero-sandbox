"""The image cache: three tiers, two bounds, and one engine call per listing.

Discovery cost is linear in the images an `images` listing matches, so every helper here is
measured in ENGINE CALLS, not just in correctness.
"""
from __future__ import annotations

from pathlib import Path

from conftest import dynamic


def test_the_image_cache_prefers_ram_then_local_then_the_engine(tmp_path, monkeypatch):
    """One image, three tiers. `/health` must name the tier a session will actually launch from."""
    from sandbox import service

    present: dict[str, int] = {}
    monkeypatch.setattr(service, "runtime", lambda: "podman")
    monkeypatch.setattr(service, "_image_size",
                        lambda argv, image: present.get(argv[argv.index("--root") + 1]
                                                        if "--root" in argv else "engine"))
    monkeypatch.setenv("ZERO_RAM_IMAGE_STORE", str(tmp_path / "ram"))
    monkeypatch.setenv("ZERO_BUILD_IMAGE_STORE", "/local")
    assert service._cache_state("d" * 64)["tier"] == "none"
    present["/local/containers/storage"] = 11
    assert service._cache_state("d" * 64) == {
        "image": "zero-runtime:dddddddddddddddd", "tier": "local", "bytes": 11}
    # The RAM tier is one private store PER IMAGE, so it is read from the filesystem: asking the
    # engine costs a listing per runtime, and /health asks about every one of them.
    ram = service._ram_store("zero-runtime:dddddddddddddddd")
    present[ram + "/containers/storage"] = 22
    assert service._cache_state("d" * 64)["tier"] == "local"
    Path(ram, "containers/storage").mkdir(parents=True)
    assert service._cache_state("d" * 64)["tier"] == "ram"

def test_the_ram_tier_is_probed_without_an_engine_call(tmp_path, monkeypatch):
    """Absence is the common case and must be a stat; only a staged runtime pays for a listing."""
    from sandbox import service

    monkeypatch.setenv("ZERO_RAM_IMAGE_STORE", str(tmp_path))
    assert service._ram_present("zero-runtime:aa") == ""
    store = service._ram_store("zero-runtime:aa")
    Path(store, "containers/storage").mkdir(parents=True)
    assert service._ram_present("zero-runtime:aa") == store
    monkeypatch.delenv("ZERO_RAM_IMAGE_STORE")
    assert service._ram_present("zero-runtime:aa") == ""

def test_ram_cache_is_size_bounded_lru_and_never_evicts_active_entries():
    from sandbox.service import _ram_victims

    entries = {
        ("old",): {"ram_bytes": 4, "last_used": 1, "active": 0},
        ("busy",): {"ram_bytes": 4, "last_used": 0, "active": 1},
        ("new",): {"ram_bytes": 3, "last_used": 2, "active": 0},
    }
    assert _ram_victims(entries, ("want",), 5, 12) == [("old",)]
    assert _ram_victims(entries, ("want",), 9, 12) is None
    assert _ram_victims(entries, ("want",), 13, 12) is None
    assert _ram_victims(entries, ("old",), 4, 12) == []

def test_the_disk_tier_is_bounded_and_never_evicts_what_is_in_use():
    """Discovery cost is linear in the images kept, so the disk tier needs a budget like RAM's."""
    from sandbox.service import _disk_victims

    images = [(f"zero-runtime:{i:016x}", float(i)) for i in range(6)]
    assert _disk_victims(images, keep=6, protected=set()) == []
    assert _disk_victims(images, keep=4, protected=set()) == [
        "zero-runtime:0000000000000000", "zero-runtime:0000000000000001"]
    # Anything a session or a cache entry still refers to survives, whatever its age.
    kept = {"zero-runtime:0000000000000000", "zero-runtime:0000000000000001"}
    assert _disk_victims(images, keep=4, protected=kept) == [
        "zero-runtime:0000000000000002", "zero-runtime:0000000000000003"]
    assert _disk_victims(images, keep=0, protected=set()) == []
    assert _disk_victims(images, keep=2, protected={n for n, _ in images}) == []
    # A protected name that is not in the listing costs nothing to keep. Counting it made three
    # owned images with keep=2 surrender two of them where only one was ever surplus.
    assert _disk_victims(images[:3], keep=2, protected={"swebench/instance:1"}) == [
        "zero-runtime:0000000000000000"]

def test_the_image_listing_is_scoped_to_what_this_service_built(monkeypatch):
    """An unfiltered listing costs time linear in every image on a shared host."""
    from sandbox import service

    seen = []
    monkeypatch.setattr(service.subprocess, "run", lambda argv, **kw: seen.append(argv) or
                        type("R", (), {"returncode": 0, "stdout": "zero-runtime:aa\n"})())
    service._forget()
    assert service._tags(["docker"]) == {"zero-runtime:aa"}
    scoped = [seen[0][i + 1] for i, x in enumerate(seen[0]) if x == "--filter"]
    assert scoped == [f"reference={p}" for p in service._OWNED_PREFIXES]
    # A request-supplied instance image is outside that scope and must be probed directly.
    service._forget()
    seen.clear()
    service._image_size(["docker"], "swebench/instance:1")
    assert all("images" not in argv for argv in seen), seen
    assert any("inspect" in argv for argv in seen), seen

def test_sizes_are_keyed_by_reported_tag_not_argument_order(monkeypatch):
    """One image missing makes the engine skip a line; a positional read mis-sizes the rest."""
    from sandbox import service

    service._forget()
    monkeypatch.setattr(service, "_tags", lambda argv: {"zero-runtime:aa", "zero-runtime:bb",
                                                        "zero-runtime:cc"})
    monkeypatch.setattr(service.subprocess, "run", lambda argv, **kw: type("R", (), {
        "returncode": 1, "stdout": "[zero-runtime:aa]\t11\n[zero-runtime:cc]\t33\n"})())
    assert service._image_size(["docker"], "zero-runtime:aa") == 11
    assert service._image_size(["docker"], "zero-runtime:cc") == 33
    assert service._image_size(["docker"], "zero-runtime:bb") is None

def test_owned_images_asks_the_engine_only_about_its_own(monkeypatch):
    """One listing, filtered to this service's own prefix, with creation times parsed."""
    from sandbox import service

    seen = []

    def fake(argv, **kw):
        seen.append(argv)
        return type("R", (), {"returncode": 0, "stdout":
                              "zero-runtime:aa\t2026-08-28 15:14:33 +0800 +08\n"
                              "zero-runtime:bb\t<none>\n"
                              "zero-runtime:<none>\t2026-08-28 15:14:34 +0800 +08\n"})()

    monkeypatch.setattr(service.subprocess, "run", fake)
    out = service._owned_images("docker")
    assert [name for name, _ in out] == ["zero-runtime:aa", "zero-runtime:bb"]
    assert out[0][1] > 0 and out[1][1] == 0.0
    assert "--filter" in seen[0] and f"reference={service._IMAGE_PREFIX}" in seen[0]

def test_a_cached_image_is_never_rebuilt(tmp_path, monkeypatch):
    """What survives a restart is the ENGINE's image, so `_build` must not reach `build` for it."""
    from sandbox import service

    argv = []
    monkeypatch.setattr(service, "_LOCAL_ROOT", str(tmp_path))
    monkeypatch.setattr(service, "root", lambda: tmp_path)
    monkeypatch.setattr(service, "_stage", lambda *a: 0)
    monkeypatch.setattr(service, "subprocess", type("S", (), {
        "run": staticmethod(lambda a, **kw: argv.append(a) or type(
            "R", (), {"returncode": 0, "stdout": "", "stderr": ""})())}))
    sub = service.resolve({"dockerfile": dynamic("warm")})
    monkeypatch.setattr(service, "_image_size", lambda a, i: 4321)
    assert service._build(sub, "docker") == ("zero-runtime:" + sub.digest[:16], 0)
    assert not argv
    monkeypatch.setattr(service, "_image_size", lambda a, i: None)
    monkeypatch.setattr(service, "_inspect", lambda e, i: False)
    service._build(sub, "docker")
    assert argv and "build" in argv[0]

def test_building_a_submitted_dockerfile_is_bounded(monkeypatch):
    from sandbox import service

    monkeypatch.setenv("ZERO_BUILD_CPUS", "3")
    monkeypatch.setenv("ZERO_BUILD_MEM_GB", "6")
    monkeypatch.setenv("ZERO_BUILD_DISK_GB", "4")
    limits = service._build_limits("podman")
    assert limits == ["--memory", "6g", "--cpu-period", "100000", "--cpu-quota", "300000",
                      "--ulimit", f"fsize={4 * 2**30}:{4 * 2**30}",
                      "--ulimit", "nproc=2048:2048"]
