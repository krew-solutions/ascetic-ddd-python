"""Example activities for the travel booking saga.

This module contains example implementations of saga activities
for a travel booking scenario, demonstrating:
- ReserveCarActivity: Low-risk, easily cancellable
- ReserveHotelActivity: Moderate risk, cancellable until check-in
- ReserveFlightActivity: High risk, strict refund policies
- FailingReserveFlightActivity: Always fails, for testing compensation

The activities are ordered by risk (least risky first) to minimize
the need for compensation when failures occur.
"""

from ascetic_ddd.saga.examples.reserve_car_activity import ReserveCarActivity
from ascetic_ddd.saga.examples.reserve_flight_activity import (
    FailingReserveFlightActivity,
    ReserveFlightActivity,
)
from ascetic_ddd.saga.examples.reserve_hotel_activity import ReserveHotelActivity

# Note: serialization_example is intentionally NOT re-exported here. It is
# a runnable demo intended to be used either via `python -m ...` or imported
# explicitly by its full path. Re-exporting it would trigger a RuntimeWarning
# when running as a script, since the package import would load it before
# runpy gets a chance to execute it.


__all__ = (
    'FailingReserveFlightActivity',
    'ReserveCarActivity',
    'ReserveFlightActivity',
    'ReserveHotelActivity',
)
