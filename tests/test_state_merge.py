"""Tests for gwmsmon.state._merge_partial and _parse_desired_sites.

The job-chunk worker pool returns N partial snapshots; the parent
deep-merges them into the final snapshot. The merge is deceptively
generic — wrong rules would either silently corrupt totals (sum where
we should keep first) or lose data (overwrite where we should sum).

DESIRED_Sites in production classads can be either a comma-separated
string OR a Python list (after classad_to_python conversion). The parse
helper must accept both shapes and produce a clean list of site names.
"""

from gwmsmon.state import _merge_partial, _parse_desired_sites

# --- _parse_desired_sites ---

def test_parse_desired_sites_string():
    assert _parse_desired_sites(
        "T1_DE_KIT,T2_CH_CERN,T2_US_MIT"
    ) == ["T1_DE_KIT", "T2_CH_CERN", "T2_US_MIT"]


def test_parse_desired_sites_string_with_whitespace():
    assert _parse_desired_sites(
        "T1_DE_KIT, T2_CH_CERN ,T2_US_MIT"
    ) == ["T1_DE_KIT", "T2_CH_CERN", "T2_US_MIT"]


def test_parse_desired_sites_list():
    assert _parse_desired_sites(
        ["T1_DE_KIT", "T2_CH_CERN", "T2_US_MIT"]
    ) == ["T1_DE_KIT", "T2_CH_CERN", "T2_US_MIT"]


def test_parse_desired_sites_list_strips_whitespace():
    assert _parse_desired_sites(
        [" T1_DE_KIT", "T2_CH_CERN ", "  T2_US_MIT "]
    ) == ["T1_DE_KIT", "T2_CH_CERN", "T2_US_MIT"]


def test_parse_desired_sites_list_filters_empty():
    assert _parse_desired_sites(
        ["T1_DE_KIT", "", "T2_CH_CERN", "  "]
    ) == ["T1_DE_KIT", "T2_CH_CERN"]


def test_parse_desired_sites_none():
    assert _parse_desired_sites(None) == []


def test_parse_desired_sites_empty_string():
    assert _parse_desired_sites("") == []


def test_parse_desired_sites_empty_list():
    assert _parse_desired_sites([]) == []


def test_parse_desired_sites_unexpected_type():
    """Defensive: if a classad ever produces an unexpected type,
    don't crash — return an empty list."""
    assert _parse_desired_sites(42) == []
    assert _parse_desired_sites({"a": 1}) == []


# --- _merge_partial ---


def test_int_default_sums():
    dst = {"Running": 5, "Held": 0}
    _merge_partial(dst, {"Running": 3, "Held": 2})
    assert dst == {"Running": 8, "Held": 2}


def test_min_key_uses_min_not_sum():
    """_priority["_min"] must track the lowest priority across workers,
    not the sum."""
    dst = {"_min": 5}
    _merge_partial(dst, {"_min": 3})
    assert dst == {"_min": 3}
    _merge_partial(dst, {"_min": 7})
    assert dst == {"_min": 3}


def test_first_only_int_keys_preserve_dst():
    """_debug[cfg_key] stores config attrs (WallTime/Memory/Cpus) that
    happen to be ints. Same value across workers — must NOT sum."""
    dst = {"WallTime": 600, "Memory": 4000, "Cpus": 4}
    _merge_partial(dst, {"WallTime": 600, "Memory": 4000, "Cpus": 4})
    assert dst == {"WallTime": 600, "Memory": 4000, "Cpus": 4}


def test_str_keeps_dst():
    """Owner/Schedd/DesiredSites are strs that workers all set to the
    same value — keep first wins."""
    dst = {"Owner": "alice"}
    _merge_partial(dst, {"Owner": "alice"})
    assert dst == {"Owner": "alice"}


def test_new_key_assigned():
    dst = {}
    _merge_partial(dst, {"Running": 1, "Owner": "bob"})
    assert dst == {"Running": 1, "Owner": "bob"}


def test_nested_dicts_recurse():
    dst = {"sites": {"T1_US": {"Running": 2, "Held": 0}}}
    _merge_partial(dst, {"sites": {"T1_US": {"Running": 3},
                                    "T2_CH": {"Running": 5}}})
    assert dst == {"sites": {"T1_US": {"Running": 5, "Held": 0},
                              "T2_CH": {"Running": 5}}}


def test_list_of_int_element_wise_sum():
    """univ_counts has {schedd: {bucket: [running, idle, held]}}."""
    dst = {"vanilla": [1, 2, 3]}
    _merge_partial(dst, {"vanilla": [10, 20, 30]})
    assert dst == {"vanilla": [11, 22, 33]}


def test_nested_list_of_int_summed():
    dst = {"x": [[1, 2], [3, 4]]}
    _merge_partial(dst, {"x": [[10, 20], [30, 40]]})
    assert dst == {"x": [[11, 22], [33, 44]]}


def test_list_length_mismatch_keeps_dst():
    """Defensive: if list lengths differ, don't half-merge."""
    dst = {"x": [1, 2, 3]}
    _merge_partial(dst, {"x": [10, 20]})
    assert dst == {"x": [1, 2, 3]}


def test_type_mismatch_keeps_dst():
    """If dst has a string and src has an int (shouldn't happen but
    defensive), keep dst rather than corrupt."""
    dst = {"x": "alice"}
    _merge_partial(dst, {"x": 7})
    assert dst == {"x": "alice"}


def test_priority_subtree():
    """Realistic shape from _aggregate_prodview: per-priority structure
    with both a "_min" (min) and per-prio counts (sum)."""
    dst = {"_priority": {"_min": 50, "_jobs": {"B7": 3, "B8": 1}}}
    _merge_partial(dst, {"_priority": {"_min": 30,
                                        "_jobs": {"B7": 5, "B9": 2}}})
    assert dst == {"_priority": {"_min": 30,
                                  "_jobs": {"B7": 8, "B8": 1, "B9": 2}}}


def test_realistic_view_totals_merge():
    """Workers each accumulate view["totals"] for their chunk. Parent
    merges to get the same totals as the sequential code would have."""
    parent = {"Running": 0, "MatchingIdle": 0, "Held": 0,
              "CpusInUse": 0, "CpusPending": 0}
    workers = [
        {"Running": 100, "MatchingIdle": 50, "Held": 5,
         "CpusInUse": 400, "CpusPending": 200},
        {"Running": 200, "MatchingIdle": 30, "Held": 2,
         "CpusInUse": 800, "CpusPending": 120},
        {"Running": 50, "MatchingIdle": 10, "Held": 0,
         "CpusInUse": 200, "CpusPending": 40},
    ]
    for w in workers:
        _merge_partial(parent, w)
    assert parent == {"Running": 350, "MatchingIdle": 90, "Held": 7,
                      "CpusInUse": 1400, "CpusPending": 360}


def test_metadata_first_non_empty_wins():
    """Worker A captures _metadata first with empty value (None), worker
    B with real value. The "if val is not None" filter in the aggregator
    means worker A's _metadata never has the key; worker B's does. After
    merge, the real value from B is in dst."""
    dst = {"_metadata": {"CMS_JobType": "Production"}}
    _merge_partial(dst, {"_metadata": {"CMS_CampaignName": "Run3"}})
    assert dst == {"_metadata": {"CMS_JobType": "Production",
                                  "CMS_CampaignName": "Run3"}}
