"""Business validation: the rules Avro's type system cannot express.

Every failure here must be a PermanentError, because that classification is
what routes the record straight to the DLQ instead of burning retries on a
record that will fail identically every time.
"""

import pytest

from src.consumer import MAX_PLAUSIBLE_PRICE, validate_order
from src.errors import PermanentError, TransientError, ValidationError


def order(**overrides):
    base = {"orderId": "1001", "product": "Item1", "price": 49.99}
    base.update(overrides)
    return base


def test_accepts_a_well_formed_order():
    validate_order(order())


@pytest.mark.parametrize("price", [0.01, 1.0, 49.99, 999_999.0])
def test_accepts_prices_across_the_valid_range(price):
    validate_order(order(price=price))


# --- rejections -------------------------------------------------------------

@pytest.mark.parametrize("price", [-0.01, -1.0, -999.99])
def test_rejects_negative_price(price):
    with pytest.raises(ValidationError, match="must be positive"):
        validate_order(order(price=price))


def test_rejects_zero_price():
    with pytest.raises(ValidationError, match="must be positive"):
        validate_order(order(price=0.0))


def test_rejects_price_above_the_sanity_ceiling():
    with pytest.raises(ValidationError, match="sanity ceiling"):
        validate_order(order(price=MAX_PLAUSIBLE_PRICE + 1))


@pytest.mark.parametrize("price", [float("nan"), float("inf"), float("-inf")])
def test_rejects_non_finite_price(price):
    with pytest.raises(ValidationError, match="finite"):
        validate_order(order(price=price))


@pytest.mark.parametrize("order_id", ["", "   "])
def test_rejects_blank_order_id(order_id):
    with pytest.raises(ValidationError, match="orderId is empty"):
        validate_order(order(orderId=order_id))


@pytest.mark.parametrize("product", ["", "  "])
def test_rejects_blank_product(product):
    with pytest.raises(ValidationError, match="product is empty"):
        validate_order(order(product=product))


def test_rejects_missing_price_field():
    payload = order()
    del payload["price"]
    with pytest.raises(ValidationError):
        validate_order(payload)


# --- classification is what drives the routing decision ---------------------

def test_validation_error_is_permanent_not_transient():
    """This is the assertion that keeps poison records out of the retry loop."""
    assert issubclass(ValidationError, PermanentError)
    assert not issubclass(ValidationError, TransientError)


def test_error_message_names_the_offending_order():
    with pytest.raises(ValidationError, match="1234"):
        validate_order(order(orderId="1234", price=-5.0))
