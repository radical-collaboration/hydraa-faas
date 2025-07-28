import statistics
from collections import defaultdict

class DataAggregator:
    """
    aggregates and summarizes data
    """
    def generate_summary(self, records):
        """
        generate summary statistics
        """
        if not records:
            return {}
        
        values = [r['value'] for r in records]
        categories = defaultdict(int)
        
        for record in records:
            categories[record['value_category']] += 1
        
        summary = {
            'count': len(records),
            'sum': sum(values),
            'mean': statistics.mean(values),
            'median': statistics.median(values),
            'min': min(values),
            'max': max(values),
            'categories': dict(categories)
        }
        
        if len(values) > 1:
            summary['stdev'] = statistics.stdev(values)
        
        return summary
