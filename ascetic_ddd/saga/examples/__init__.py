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


__all__ = (
    'FailingReserveFlightActivity',
    'ReserveCarActivity',
    'ReserveFlightActivity',
    'ReserveHotelActivity',
)
