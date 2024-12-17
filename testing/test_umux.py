import pandas as pd
import numpy as np

def calculate_umux_score(row):
    # Get the four UMUX items
    q1 = row['Die Funktionen von Go Wandr erfüllen meine Anforderungen.']
    q2 = row['Die Nutzung von Go Wandr ist eine frustrierende Erfahrung.']
    q3 = row['Go Wandr ist einfach zu bedienen.']
    q4 = row['Ich muss viel Zeit aufwenden, um Fehler von Go Wandr zu korrigieren.']
    
    # Calculate according to the standardized formula:
    # ((Q1 - 1) + (Q3 - 1) + (7 - Q2) + (7 - Q4)) × (100/24)
    score = ((q1 - 1) + (q3 - 1) + (7 - q2) + (7 - q4)) * (100/24)
    
    return score

def main():
    # Read the CSV file
    df = pd.read_csv('tests/ux_test.csv')
    
    # Calculate individual UMUX scores
    individual_scores = df.apply(calculate_umux_score, axis=1)
    
    # Calculate overall UMUX score (mean of individual scores)
    overall_score = np.mean(individual_scores)
    
    # Print results
    print("\nIndividual UMUX Scores:")
    for i, score in enumerate(individual_scores, 1):
        print(f"Participant {i}: {score:.2f}")
    
    print(f"\nOverall UMUX Score: {overall_score:.2f}")
    
    # Additional statistics
    print(f"\nStandard Deviation: {np.std(individual_scores):.2f}")
    print(f"Minimum Score: {np.min(individual_scores):.2f}")
    print(f"Maximum Score: {np.max(individual_scores):.2f}")

if __name__ == "__main__":
    main()
