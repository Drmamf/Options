#!/usr/bin/env python3
from decimal import Decimal as D

import ReyT_short_premium_engine as s


def assert_close(a, b, tol=D("0.0001")):
    a = D(str(a))
    b = D(str(b))
    if abs(a - b) > tol:
        raise AssertionError(f"{a} != {b}")


def main():
    # Classic short straddle: K=100, total net credit=10 -> BE 90 / 110.
    legs = [
        {"option_type": "PUT", "strike": D("100"), "contract_size": 1, "qty": 1},
        {"option_type": "CALL", "strike": D("100"), "contract_size": 1, "qty": 1},
    ]
    lo, hi = s.full_position_breakevens(D("10"), legs)
    assert_close(lo, D("90"))
    assert_close(hi, D("110"))

    # Short strangle: Kp=95, Kc=105, net credit=8 -> BE 87 / 113.
    legs = [
        {"option_type": "PUT", "strike": D("95"), "contract_size": 1, "qty": 1},
        {"option_type": "CALL", "strike": D("105"), "contract_size": 1, "qty": 1},
    ]
    lo, hi = s.full_position_breakevens(D("8"), legs)
    assert_close(lo, D("87"))
    assert_close(hi, D("113"))

    # Full-position BE after an extra call adjustment.
    # Short P100 + C100, cash 10; then short C110, +3 => upper BE = 111.5.
    legs = [
        {"option_type": "PUT", "strike": D("100"), "contract_size": 1, "qty": 1},
        {"option_type": "CALL", "strike": D("100"), "contract_size": 1, "qty": 1},
        {"option_type": "CALL", "strike": D("110"), "contract_size": 1, "qty": 1},
    ]
    lo, hi = s.full_position_breakevens(D("13"), legs)
    assert_close(hi, D("111.5"))

    side, dist = s.loss_distance(D("115.5"), D("90"), D("110"))
    if side != "CALL":
        raise AssertionError(side)
    assert_close(dist, D("5"))

    # No loss distance while spot is inside the BE range.
    side, dist = s.loss_distance(D("100"), D("90"), D("110"))
    if side is not None or dist != 0:
        raise AssertionError((side, dist))

    print("SHORT_PREMIUM_MATH_SELFTEST_OK")


if __name__ == "__main__":
    main()
