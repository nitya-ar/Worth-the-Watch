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


## Visual Insights

<table>
  <tr>
    <td align="center">
      <img src="Visuals/Value%20Index%20Bar%20Chart.png" width="200"/><br>
      <b>Value Index</b><br>
      Overall value score.
    </td>
    <td align="center">
      <img src="Visuals/Heatmap%20–%20Normalized%20Metrics.png" width="200"/><br>
      <b>Metric Breakdown</b><br>
      Scores by metric.
    </td>
  </tr>
  <tr>
    <td align="center">
      <img src="Visuals/IMDb%20Score%20per%20Dollar%20(Lollipop%20Chart).png" width="200"/><br>
      <b>IMDb/$</b><br>
      Quality per dollar.
    </td>
    <td align="center">
      <img src="Visuals/IMDb%20vs%20Titles%20per%20Dollar%20(Bubble%20Chart).png" width="200"/><br>
      <b>Titles vs Rating</b><br>
      Quantity vs quality.
    </td>
  </tr>
  <tr>
    <td align="center">
      <img src="Visuals/Originals%20per%20Dollar%20(Donut%20Chart).png" width="200"/><br>
      <b>Originals/$</b><br>
      Original content efficiency.
    </td>
    <td align="center">
      <img src="Visuals/Total%20Titles%20—%20Amazon%20vs%20Others.png" width="200"/><br>
      <b>Catalog Size</b><br>
      Total titles.
    </td>
  </tr>
</table>

## Summary of Findings

1. **Apple TV Plus** – Best overall value: low price, top-rated content, highly efficient.
2. **Netflix** – Strong in originals: high cost but unmatched exclusive content.
3. **Prime Video** – Huge catalog: best for volume, average quality, few originals.
4. **Max** – High-quality but small library: solid shows, poor cost efficiency.
5. **Hulu** – Weakest value: most expensive, small catalog, few originals.

## Recommendations

- **Best Duo**: Apple TV+ + Netflix – Prestige + originals at good value.
- **Core Option**: Use Prime Video – Great variety; add others for uniqueness.
- **Want Exclusives?** Netflix solo – Top platform for new original content.
- **Avoid Hulu Solo** – Not worth it unless part of a bundle deal.
- **Seasonal Sub**: Use Max when shows drop – Cancel when done.
- **Top Pick**: Apple TV+ – Cheapest, most efficient, critically acclaimed.

## Medium Article

For a complete breakdown of the data, methodology, and insights, check out the Medium article:

**Read on Medium**  
*[Medium](https://medium.com/@nitya.4r/which-streaming-platform-offers-the-best-value-in-2025-2bd3b75882ed)*

## Author

Nitya Arya  
[GitHub](https://github.com/nitya-ar)  
[Medium](https://medium.com/@nitya.4r/which-streaming-platform-offers-the-best-value-in-2025-2bd3b75882ed)
[LinkedIn](https://www.linkedin.com/in/nitya-arya/)
