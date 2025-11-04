"""
Market Segment Profiles

Detailed descriptions and characteristics of each customer segment
for the Betashares ETF trading platform.
"""

SEGMENT_PROFILES = {
    'Blue Chip': {
        'icon': '🏛️',
        'tagline': 'Stability Seekers',
        'description': 'Conservative investors focused on established, large-cap companies with proven track records. These customers prioritize capital preservation and steady dividend income over aggressive growth.',
        'characteristics': [
            '🎯 **Investment Focus**: Large-cap index funds, dividend aristocrats, and defensive sector ETFs',
            '📊 **Risk Profile**: Low to moderate risk tolerance with focus on steady returns',
            '💰 **Average Portfolio**: Higher account balances with long-term holding periods',
            '🔍 **Research Habits**: Thorough due diligence, values company fundamentals and financial stability',
            '⏱️ **Investment Horizon**: Long-term (5+ years), buy-and-hold strategy'
        ],
        'typical_products': 'ASX 200 Index ETFs, Global dividend funds, Investment-grade bond ETFs',
        'engagement_strategy': 'Provide in-depth research reports, dividend reinvestment programs, and portfolio rebalancing tools',
        'color': '#1976d2'  # Blue
    },
    'Crypto': {
        'icon': '₿',
        'tagline': 'Digital Asset Enthusiasts',
        'description': 'Tech-savvy investors embracing the digital asset revolution. These customers seek exposure to blockchain technology, cryptocurrencies, and Web3 innovations through regulated ETF vehicles.',
        'characteristics': [
            '🎯 **Investment Focus**: Bitcoin ETFs, blockchain technology funds, and digital innovation ETFs',
            '📊 **Risk Profile**: High risk tolerance with appetite for volatility and emerging technologies',
            '💰 **Average Portfolio**: Moderate balances with frequent trading activity and portfolio adjustments',
            '🔍 **Research Habits**: Active on social media, follows crypto influencers and technical analysis',
            '⏱️ **Investment Horizon**: Short to medium-term (6 months - 3 years), tactical trading approach'
        ],
        'typical_products': 'Bitcoin ETFs, Ethereum exposure funds, Blockchain technology ETFs, Digital payments funds',
        'engagement_strategy': 'Real-time market alerts, mobile-first experience, educational content on digital assets',
        'color': '#f57c00'  # Orange/Gold
    },
    'Social Impact': {
        'icon': '🤝',
        'tagline': 'Values-Driven Investors',
        'description': 'Purpose-driven investors who align their portfolios with personal values. These customers seek financial returns while making a positive impact on society through ethical and responsible investing.',
        'characteristics': [
            '🎯 **Investment Focus**: ESG funds, impact investing ETFs, diversity & inclusion focused products',
            '📊 **Risk Profile**: Moderate risk tolerance with strong emphasis on values alignment',
            '💰 **Average Portfolio**: Balanced allocations with focus on transparent impact metrics',
            '🔍 **Research Habits**: Scrutinizes company practices, ESG ratings, and social impact reports',
            '⏱️ **Investment Horizon**: Medium to long-term (3-7 years), patient capital approach'
        ],
        'typical_products': 'Gender diversity ETFs, Clean energy funds, Ethical screened portfolios, Microfinance funds',
        'engagement_strategy': 'Impact reporting dashboards, ESG scoring tools, community forums for values-based investing',
        'color': '#7cb342'  # Green
    },
    'Sustainability Focused': {
        'icon': '🌱',
        'tagline': 'Green Future Builders',
        'description': 'Environmentally conscious investors committed to funding the transition to a sustainable economy. These customers prioritize climate solutions, renewable energy, and companies actively reducing their environmental footprint.',
        'characteristics': [
            '🎯 **Investment Focus**: Climate change ETFs, renewable energy funds, sustainable resources, green bonds',
            '📊 **Risk Profile**: Moderate to high risk tolerance for long-term environmental transition opportunities',
            '💰 **Average Portfolio**: Growing allocations to climate solutions with thematic focus',
            '🔍 **Research Habits**: Deep analysis of carbon footprints, net-zero commitments, and environmental metrics',
            '⏱️ **Investment Horizon**: Long-term (7+ years), aligned with global sustainability targets'
        ],
        'typical_products': 'Clean energy ETFs, Water resource funds, Circular economy ETFs, Low carbon index funds',
        'engagement_strategy': 'Carbon footprint tracking, sustainability impact metrics, climate transition scenario analysis',
        'color': '#388e3c'  # Dark Green
    }
}


def get_segment_profile(segment_name: str) -> dict:
    """Get profile information for a specific segment."""
    return SEGMENT_PROFILES.get(segment_name, {})


def get_all_segments() -> list:
    """Get list of all segment names."""
    return list(SEGMENT_PROFILES.keys())

