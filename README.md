# Worth-the-Watch

**A data-driven analysis comparing the value offered by major streaming platforms in 2025**

## Overview

As streaming services expand and subscription costs increase, consumers face a common challenge: determining which platforms offer the best return on investment. This project analyzes five major streaming services—Netflix, Hulu, Max, Amazon Prime Video, and Apple TV+—to identify which delivers the most value in 2025 based on content quantity, originality, quality, and pricing.

## Objective

This project aims to answer the question: **Which streaming service offers the most value for money in 2025?**

The analysis evaluates catalog size, cost, original content offerings, and IMDb ratings, and aggregates these into a composite metric called the **Value Index**.

## Key Questions

- Which platform offers the highest number of titles per dollar?
- Which service delivers the most original content relative to its cost?
- How does content quality, measured through IMDb ratings, impact value?
- What is the most cost-effective streaming service in 2025?

## Methodology

### Data Collection

- Catalog data for each platform was sourced from Kaggle.
- Titles were labeled as original or non-original using the TMDb API.
- Subscription pricing was manually collected as of 2025.

### Metrics Engineered

- **Titles per Dollar**: Total number of titles divided by monthly subscription cost.
- **Originals per Dollar**: Number of original titles divided by cost.
- **Weighted IMDb Score per Dollar**: Incorporates both rating and vote count, adjusted for price.

All metrics were normalized using Min-Max scaling to generate a composite **Value Index** for each platform.

## Project Structure

```
Worth-the-Watch/
├── data/ Contains raw and processed streaming datasets
│ ├── raw/
│ └── processed/
├── notebooks/ Jupyter Notebooks for processing and analysis
├── visuals/ Final charts and visual assets
├── requirements.txt Python libraries used
├── LICENSE Project license (MIT)
└── README.md Project overview
```

## Visual Insights

Click thumbnails to view full-size charts:

**Value Index by Platform**  
Overall value score combining price, quantity, quality, and originality.  
[<img src="Visuals/Value%20Index%20Bar%20Chart.png" width="200"/>](Visuals/Value%20Index%20Bar%20Chart.png)

**Normalized Metric Breakdown**  
How each platform scores on individual value metrics.  
[<img src="Visuals/Heatmap%20–%20Normalized%20Metrics.png" width="200"/>](Visuals/Heatmap%20–%20Normalized%20Metrics.png)

**IMDb Score per Dollar**  
Quality of content per dollar, weighted by ratings and votes.  
[<img src="Visuals/IMDb%20Score%20per%20Dollar%20(Lollipop%20Chart).png" width="200"/>](Visuals/IMDb%20Score%20per%20Dollar%20(Lollipop%20Chart).png)

**Titles vs IMDb Rating**  
Compares how many titles you get per dollar vs their average IMDb rating; bubble size shows original content count.  
[<img src="Visuals/IMDb%20vs%20Titles%20per%20Dollar%20(Bubble%20Chart).png" width="200"/>](Visuals/IMDb%20vs%20Titles%20per%20Dollar%20(Bubble%20Chart).png)

**Originals per Dollar**  
Number of original titles per dollar of subscription cost.  
[<img src="Visuals/Originals%20per%20Dollar%20(Donut%20Chart).png" width="200"/>](Visuals/Originals%20per%20Dollar%20(Donut%20Chart).png)

**Total Titles by Platform**  
How many titles each platform offers overall.  
[<img src="Visuals/Total%20Titles%20—%20Amazon%20vs%20Others.png" width="200"/>](Visuals/Total%20Titles%20—%20Amazon%20vs%20Others.png)

## Visual Insights

<details>
<summary>Click to expand key charts</summary>

<table>
  <tr>
    <td><img src="Visuals/Value%20Index%20Bar%20Chart.png" width="200"/><br><b>Value Index</b><br>Overall value score.</td>
    <td><img src="Visuals/Heatmap%20–%20Normalized%20Metrics.png" width="200"/><br><b>Metric Breakdown</b><br>Scores by metric.</td>
  </tr>
  <tr>
    <td><img src="Visuals/IMDb%20Score%20per%20Dollar%20(Lollipop%20Chart).png" width="200"/><br><b>IMDb/$</b><br>Quality per dollar.</td>
    <td><img src="Visuals/IMDb%20vs%20Titles%20per%20Dollar%20(Bubble%20Chart).png" width="200"/><br><b>Titles vs Rating</b><br>Quantity vs quality.</td>
  </tr>
  <tr>
    <td><img src="Visuals/Originals%20per%20Dollar%20(Donut%20Chart).png" width="200"/><br><b>Originals/$</b><br>Original content efficiency.</td>
    <td><img src="Visuals/Total%20Titles%20—%20Amazon%20vs%20Others.png" width="200"/><br><b>Catalog Size</b><br>Total titles.</td>
  </tr>
</table>

</details>

## Summary of Findings

1. **Apple TV Plus** - Ranks first in overall value. Delivers consistently top-rated content at the lowest subscription price. A smaller catalog is offset by strong critical acclaim and high efficiency.  
2. **Netflix** - Ranks second. Leads in original content volume and maintains strong value overall. Despite a higher price, its investment in exclusives boosts its performance.  
3. **Amazon Prime Video** - Ranks third. Offers the largest catalog by far with thousands of titles per dollar. However, average ratings and fewer originals limit its overall value.  
4. **Max** - Ranks fourth. Known for high-quality content but has fewer titles and originals than competitors. The subscription price outweighs the depth of its library.  
5. **Hulu** - Ranks last. Has the most expensive plan with modest catalog size and minimal original content. Provides the lowest return for cost.

## Recommendations

- **Combine Apple TV Plus and Netflix** - This duo delivers maximum value. Apple TV Plus offers prestige at a low cost, while Netflix brings a broad slate of original content.  
- **Use Amazon Prime Video as your core service** - Best for general variety and volume. Add other platforms to complement originality or critical acclaim.  
- **Subscribe to Netflix solo if you want originals** - No platform outperforms Netflix in exclusive content. Ideal for users who want the latest shows first.  
- **Skip Hulu unless it comes in a bundle** - Standalone value is weak. Only worthwhile if part of a discounted package.  
- **Use Max as a seasonal subscription** - Great for specific high-quality shows. Subscribe when needed, cancel when content runs out.  
- **Pick Apple TV Plus for top value** - Best all-around option. Balances low cost with critically acclaimed content.

## Medium Article

For a complete breakdown of the data, methodology, and insights, check out the Medium article:

**Read on Medium**  
*(Insert article link here)*


## Author

Nitya Arya  
[GitHub](https://github.com/nitya-ar)  
[Medium](https://medium.com/@your-medium-profile)
[LinkedIn](https://www.linkedin.com/in/nitya-arya/)
