import pandas as pd
import matplotlib.pyplot as plt

# Read the CSV file
df = pd.read_csv('tests/Hike.csv')

# Filter out null ratings and ensure ratings are between 1-5
valid_ratings = df[df['rating'].notna() & (df['rating'].between(1, 5))]

# Create the plot
plt.figure(figsize=(10, 6))

# Create bar plot with counts
counts = valid_ratings['rating'].value_counts().sort_index()
bars = plt.bar(counts.index, counts.values, color='skyblue', edgecolor='black')

# Customize the plot
plt.title('Distribution of Hike Ratings', pad=20, fontsize=14)
plt.xlabel('Rating (1-5)', fontsize=12)
plt.ylabel('Number of Ratings', fontsize=12)

# Add value labels on top of each bar
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'{int(height)}',
             ha='center', va='bottom')

# Set x-axis ticks to whole numbers
plt.xticks(range(1, 6))

# Add grid for better readability
plt.grid(True, axis='y', linestyle='--', alpha=0.7)

# Adjust layout and display
plt.tight_layout()

# Save the plot
plt.savefig('tests/rating_distribution.png')
plt.close()
