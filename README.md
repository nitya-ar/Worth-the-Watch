# Worth-the-Watch

A data-driven analysis comparing the value offered by major streaming platforms in 2025.

## Objective

With the rapid growth of streaming services and rising subscription prices, consumers often struggle to decide which platform delivers the best return on investment. This project addresses that challenge using real data, with the goal of identifying which platform offers the most value for money.

## Key Questions

- Which platform provides the highest number of titles per dollar?
- Which platform offers the most original content relative to its cost?
- How does content quality, measured by IMDb ratings, influence value?
- What is the most cost-effective streaming service in 2025?

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

## Methodology

- Streaming catalog datasets for Netflix, Hulu, Max, Amazon Prime Video, and Apple TV+ were sourced from Kaggle
- Each title was labeled as either original or non-original using the TMDb API
- Monthly pricing data was collected manually for each platform
- Several value metrics were engineered:
  - Titles per dollar
  - Originals per dollar
  - Weighted IMDb rating per dollar (factoring in both score and vote count)
- All metrics were normalized using Min-Max scaling to compute a composite Value Index

## Visual Insights

### Value Index by Platform
[Insert image: visuals/value_index_barplot.png]

### Titles Per Dollar
[Insert image: visuals/titles_per_dollar.png]

### Originals Per Dollar
[Insert image: visuals/originals_per_dollar.png]

## Summary of Findings

- Apple TV+ offers the highest overall value due to strong ratings and low cost
- Netflix has the largest number of original titles and ranks high in value
- Amazon Prime Video offers the most titles overall and performs well in cost-efficiency
- Max offers highly rated content but provides fewer titles, resulting in lower overall value
- Hulu delivers moderate content value but has the lowest value index when price is factored in

## Recommendations

- Choose Apple TV+ for high-quality content at a low cost
- Subscribe to Netflix if you prioritize original and exclusive series
- Opt for Amazon Prime Video if you want the most content per dollar spent
- Consider Max for high-rated shows, but only if prestige content is your priority
- Use Hulu as part of a bundle rather than a standalone subscription

## Medium Article

A detailed walkthrough of this project is available in the Medium article:

**[Read on Medium](https://medium.com/your-article-link)**  

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
