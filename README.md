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

### Value Index by Platform

A composite measure combining all normalized metrics to assess overall platform value.

![Value Index](Visuals/Value%20Index%20Bar%20Chart.png)

### Normalized Metric Breakdown

Comparison of platforms across individual value metrics. Darker shades indicate better value.

![Heatmap](Visuals/Heatmap%20–%20Normalized%20Metrics.png)

### IMDb Score per Dollar

Comparison of weighted content quality (IMDb rating × votes) per dollar spent.

![IMDb Score per Dollar](Visuals/IMDb%20Score%20per%20Dollar%20(Lollipop%20Chart).png)

### Titles per Dollar vs IMDb Rating

Balancing catalog size and quality. Bubble size represents number of originals.

![Bubble Chart](Visuals/IMDb%20vs%20Titles%20per%20Dollar%20(Bubble%20Chart).png)

### Originals per Dollar

Proportion of original content value by platform.

![Originals per Dollar](Visuals/Originals%20per%20Dollar%20(Donut%20Chart).png)

### Total Titles by Platform

Platform-wise total content offerings.

![Total Titles](Visuals/Total%20Titles%20—%20Amazon%20vs%20Others.png)


## Summary of Findings

The analysis reveals that value in streaming is multi-dimensional—it is not just about how many titles a platform offers, but how well those titles perform and what users get for each dollar spent.

- **Apple TV+** offers the highest value overall. Despite a smaller library, it consistently delivers top-rated content at a highly competitive subscription price. Its cost-to-quality ratio is unmatched.
- **Netflix** leads in original content volume and performs strongly across all value dimensions. It is the most dominant platform in terms of exclusive productions.
- **Amazon Prime Video** has the largest content library, providing the highest number of titles per dollar. While its content quality is average, the sheer quantity drives its value up.
- **Max** is a platform with highly rated content, but its smaller catalog limits overall value efficiency. It shines in quality but lags in breadth.
- **Hulu** has the highest average IMDb rating, but its limited original content and modest catalog size reduce its value when evaluated per dollar spent.

These results are derived from a normalized comparison of catalog size, content originality, IMDb ratings, and cost.

## Recommendations

- **Apple TV+** is ideal for viewers focused on premium storytelling and critical acclaim without a high price tag.
- **Netflix** is the best choice for those who prioritize access to a wide range of exclusive and original programming.
- **Amazon Prime Video** is suited for consumers seeking content volume and general affordability.
- **Max** is recommended for audiences who prefer quality over quantity and are willing to pay a bit more for curated, high-rated content.
- **Hulu** is best considered as part of a bundled offering rather than a standalone service due to its relatively weaker value proposition.

## Medium Article

A full walkthrough of the methodology and findings is available in the Medium article:

**Read on Medium**  
*(Insert article link here)*

## How to Run

1. Clone this repository:
   git clone https://github.com/nitya-ar/Worth-the-Watch.git

2. Navigate into the project:
   cd Worth-the-Watch

3. Install dependencies:
   pip install -r requirements.txt

4. Open and run the notebooks in Jupyter or VS Code

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Author

Nitya Arya  
[GitHub](https://github.com/nitya-ar)  
[Medium](https://medium.com/@your-medium-profile)
