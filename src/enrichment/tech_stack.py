from __future__ import annotations

from typing import Any


def detect_tech_stack_from_html(html: str) -> list[str]:
    """Detect likely website technologies from homepage HTML."""
    if not html:
        return []

    html_lower = html.lower()
    detected: list[str] = []

    markers = {
        "WordPress": ["wp-content", "wp-includes", "wordpress"],
        "Elementor": ["elementor"],
        "Shopify": ["cdn.shopify.com", "shopify", "shopify-section"],
        "Wix": ["wix.com", "wixstatic.com", "_wixcssrules"],
        "Squarespace": ["squarespace", "static1.squarespace.com"],
        "Webflow": ["webflow", "webflow.io"],
        "React": ["react", "__react", "data-reactroot"],
        "Next.js": ["_next/", "__next"],
        "Bootstrap": ["bootstrap", "bootstrap.min.css"],
        "jQuery": ["jquery", "jquery.min.js"],
    }

    for tech_name, tech_markers in markers.items():
        if any(marker in html_lower for marker in tech_markers):
            detected.append(tech_name)

    return detected