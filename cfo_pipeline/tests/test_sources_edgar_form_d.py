"""Form D source — operating-company filter regression suite.

The patterns here are intentionally narrow: dropping a real operating
company because the regex over-fires is a worse failure than letting
a fund slip through (the enrichment + scoring layers will deal with it).
"""

from __future__ import annotations

from cfo_pipeline.sources import edgar_form_d


def test_extract_company_name_from_atom_title():
    title = "D - Acme Robotics Inc (1234567) (Filer)"
    assert edgar_form_d._extract_company_name(title) == "Acme Robotics Inc"


def test_extract_returns_none_for_non_form_d_title():
    assert edgar_form_d._extract_company_name("10-K - Acme (1234) (Filer)") is None


def test_drops_obvious_fund_filers():
    not_operating = [
        "Acme Ventures III LP",
        "Big Tech Capital Partners",
        "Series 2024 Holdings",
        "Family Office Investments LLC",
        "Bay Capital Management, L.P.",
        "Bancorp Holdings",
    ]
    for name in not_operating:
        assert not edgar_form_d._is_operating_company(name), name


def test_keeps_operating_company_names():
    operating = [
        "Acme Robotics Inc",
        "Genesis AI",
        "Altara Health Sciences",
        "Pioneer Legal",
        "Stripe Inc",
    ]
    for name in operating:
        assert edgar_form_d._is_operating_company(name), name


def test_drops_vintage_year_spv():
    assert not edgar_form_d._is_operating_company("Summit Ridge 2024 LLC")
    assert not edgar_form_d._is_operating_company("Pinecrest 1986 Inc")


def test_drops_street_spv():
    assert not edgar_form_d._is_operating_company("JR Hyde Park Blvd LLC")
    assert not edgar_form_d._is_operating_company("Marina Bay Avenue LLC")
