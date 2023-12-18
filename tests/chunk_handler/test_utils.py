import pytest

from chunkio.chunk_handler._utils import check_mode


@pytest.mark.parametrize(
    "mode, accepted_mode", [
        ("r", "rt"),
        ("t", "rt"),
        ("tr", "rt")
    ]
)
def test_check_mode_passes(mode: str, accepted_mode: str):
    assert check_mode(mode, accepted_mode)


@pytest.mark.parametrize(
    "mode, accepted_mode", [
        # Completely unsupported
        ("x", "rt"),
        ("a+", "rt"),
        ("wt+", "rt"),
        # Extra unsupported char
        ("rtx", "rt"),
        ("r+", "rt"),
        ("at", "rt")
    ]
)
def test_check_mode_assert(mode: str, accepted_mode: str):
    with pytest.raises(AssertionError):
        check_mode(mode, accepted_mode)