def check_mode(mode: str, accepted: str) -> bool:
    _mode_set = set(list(mode))
    _accepted_mode_set = set(list(accepted))
    if not _mode_set.difference(accepted):
        return True
    raise AssertionError(f"Provided mode '{mode}' does conflict with accepted modes: {accepted}")
