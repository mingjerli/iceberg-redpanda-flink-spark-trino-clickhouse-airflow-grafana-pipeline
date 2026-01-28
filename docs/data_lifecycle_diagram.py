#!/usr/bin/env python3
"""
Data Lifecycle Diagram Generator
=================================

Generates a diagram showing the data flow through different layers:
Raw -> Staging -> Semantic -> Core -> Analytics -> Marts

Usage:
    python data_lifecycle_diagram.py

Output:
    Generates a matplotlib-based diagram showing the data lifecycle.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch


def create_data_lifecycle_diagram():
    """Create a data lifecycle diagram showing the flow through layers."""
    fig, ax = plt.subplots(1, 1, figsize=(14, 10))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 10)
    ax.set_aspect('equal')
    ax.axis('off')

    # Colors for each layer
    colors = {
        'raw': '#ffcccb',        # Light red
        'staging': '#90EE90',    # Light green
        'semantic': '#87CEEB',   # Sky blue
        'core': '#DDA0DD',       # Plum
        'analytics': '#F0E68C',  # Khaki
        'marts': '#FFA07A',      # Light salmon
    }

    # Layer definitions
    layers = [
        {'name': 'Raw', 'y': 8.5, 'color': colors['raw'],
         'tables': ['shopify_orders', 'shopify_customers', 'stripe_charges', 'hubspot_contacts'],
         'desc': 'Append-only, original data'},
        {'name': 'Staging', 'y': 7.0, 'color': colors['staging'],
         'tables': ['stg_shopify_orders', 'stg_shopify_customers', 'stg_stripe_charges', 'stg_hubspot_contacts'],
         'desc': 'Cleaned, typed, standardized'},
        {'name': 'Semantic', 'y': 5.5, 'color': colors['semantic'],
         'tables': ['entity_index', 'blocking_index'],
         'desc': 'Entity resolution'},
        {'name': 'Core', 'y': 4.0, 'color': colors['core'],
         'tables': ['customers', 'orders'],
         'desc': 'Unified business objects'},
        {'name': 'Analytics', 'y': 2.5, 'color': colors['analytics'],
         'tables': ['customer_metrics', 'order_summary', 'payment_metrics'],
         'desc': 'Pre-computed aggregations'},
        {'name': 'Marts', 'y': 1.0, 'color': colors['marts'],
         'tables': ['customer_360', 'sales_dashboard'],
         'desc': 'Business-ready views'},
    ]

    # Draw layers
    for layer in layers:
        # Main layer box
        box = FancyBboxPatch(
            (1, layer['y'] - 0.4), 12, 1.0,
            boxstyle="round,pad=0.05,rounding_size=0.2",
            facecolor=layer['color'],
            edgecolor='black',
            linewidth=2,
            alpha=0.7
        )
        ax.add_patch(box)

        # Layer name
        ax.text(1.5, layer['y'] + 0.2, layer['name'],
                fontsize=14, fontweight='bold', va='center')

        # Description
        ax.text(1.5, layer['y'] - 0.1, layer['desc'],
                fontsize=9, style='italic', va='center', color='gray')

        # Tables
        table_text = ' | '.join(layer['tables'][:4])
        if len(layer['tables']) > 4:
            table_text += ' ...'
        ax.text(5, layer['y'] + 0.1, table_text,
                fontsize=8, va='center', family='monospace')

    # Draw arrows between layers
    for i in range(len(layers) - 1):
        y_start = layers[i]['y'] - 0.4
        y_end = layers[i + 1]['y'] + 0.6
        arrow = FancyArrowPatch(
            (7, y_start), (7, y_end),
            arrowstyle='-|>',
            mutation_scale=15,
            linewidth=2,
            color='#333333'
        )
        ax.add_patch(arrow)

    # Processing engines on the side
    engines = [
        {'name': 'Flink\n(Streaming)', 'y': 7.75, 'color': '#E6E6FA'},
        {'name': 'Spark\n(Batch)', 'y': 4.5, 'color': '#E6E6FA'},
    ]

    for eng in engines:
        box = FancyBboxPatch(
            (13.5, eng['y'] - 0.5), 1.5, 1.0,
            boxstyle="round,pad=0.05,rounding_size=0.2",
            facecolor=eng['color'],
            edgecolor='black',
            linewidth=1.5
        )
        ax.add_patch(box)
        ax.text(14.25, eng['y'], eng['name'],
                fontsize=9, ha='center', va='center')

    # Title
    ax.text(7, 9.5, 'Data Layer Lifecycle',
            fontsize=18, fontweight='bold', ha='center')

    # Legend
    legend_y = 0.2
    for i, (name, color) in enumerate(colors.items()):
        x = 1 + i * 2
        patch = mpatches.FancyBboxPatch(
            (x, legend_y), 0.3, 0.3,
            boxstyle="round,pad=0.02",
            facecolor=color,
            edgecolor='black'
        )
        ax.add_patch(patch)
        ax.text(x + 0.4, legend_y + 0.15, name.capitalize(),
                fontsize=8, va='center')

    plt.tight_layout()
    return fig


def main():
    fig = create_data_lifecycle_diagram()
    output_path = "docs/data_lifecycle.png"
    fig.savefig(output_path, dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    print(f"Generated {output_path}")
    plt.close(fig)


if __name__ == "__main__":
    main()
