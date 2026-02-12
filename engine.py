import re
import pandas as pd
import jellyfish
from collections import defaultdict
from ddgs import DDGS

class CompanyDedupEngine:
    def __init__(self, settings=None):
        self.settings = settings or {}
        self.hard_threshold = self.settings.get('hard', 0.90)
        self.soft_threshold = self.settings.get('soft', 0.85)
        self.no_subsidiary_fold = self.settings.get('no_subsidiary_fold', False)
        self.enable_web_search = self.settings.get('web_search', False)
        
        # Legal Suffixes (ordered by length descending to avoid partial matches)
        self.suffixes = sorted([
            "PRIVATE LIMITED", "PVT LTD", "PVT. LTD.", "LTD", "LIMITED", "LLC", "LLP", "PLC",
            "INC", "INCORPORATED", "CO", "CO.", "COMPANY", "PTE LTD",
            "GMBH", "GMBH & CO KG", "B.V.", "A/S", "S.A. DE C.V.", "SP Z O O", "SP ZOO",
            "S R L", "S.R.L.", "S A", "S.P.A.", "SA DE CV"
        ], key=len, reverse=True)
        
        # Country Tokens for folding
        self.countries = sorted([
            "INDIA", "USA", "UAE", "CHINA", "JAPAN", "KOREA", "SINGAPORE", "MALAYSIA", "CANADA", "BRAZIL", 
            "GERMANY", "FRANCE", "ITALY", "UNITED STATES", "UNITED KINGDOM", "HONG KONG", "NEW ZEALAND", 
            "SOUTH AFRICA", "SAUDI ARABIA", "COTE DIVOIRE"
        ], key=len, reverse=True)

        # Default Mappings
        self.acronym_map = {
            "IBM INDIA": "IBM",
            "TCS": "TATA CONSULTANCY SERVICES",
            "HDFC": "HDFC BANK"
        }
        # Add user overrides
        if 'add_map' in self.settings:
            self.acronym_map.update(self.settings['add_map'])

    def normalize(self, name):
        if not name or pd.isna(name): return ""
        # UPPER CASE
        name = str(name).upper()
        # Remove punctuation except & / -
        name = re.sub(r'[^\w\s&/-]', ' ', name)
        # Collapse spaces
        name = re.sub(r'\s+', ' ', name).strip()
        return name

    def strip_suffixes(self, name):
        prev_name = ""
        while name != prev_name:
            prev_name = name
            for sfx in self.suffixes:
                pattern = rf"\b{re.escape(sfx)}$"
                name = re.sub(pattern, "", name).strip()
        return name

    def fold_subsidiaries(self, name):
        if self.no_subsidiary_fold: return name
        prev_name = ""
        while name != prev_name:
            prev_name = name
            for country in self.countries:
                pattern = rf"\b{re.escape(country)}$"
                name = re.sub(pattern, "", name).strip()
        return name

    def web_verify(self, name):
        """Use DuckDuckGo to find the official/main company name if ambiguous."""
        if not name: return name
        try:
            with DDGS() as ddgs:
                # Search for the name + "official website" or just the name
                results = list(ddgs.text(f"{name} official company name", max_results=3))
                if results:
                    # Very simple heuristic: first result title often contains the canonical name
                    # We look for common patterns or just return the first meaningful title part
                    title = results[0]['title']
                    # Clean title (remove " - Home", " | Official Site", etc.)
                    canonical = re.split(r' - | \| |: ', title)[0].strip()
                    return canonical.upper()
        except Exception as e:
            print(f"Web search failed for {name}: {e}")
        return name

    def get_base_name(self, name):
        norm = self.normalize(name)
        base = self.strip_suffixes(norm)
        base = self.fold_subsidiaries(base)
        # Apply acronym mapping
        if base in self.acronym_map:
            base = self.acronym_map[base]
        return norm, base

    def get_block_key(self, base_name):
        if not base_name: return "NONE"
        tokens = base_name.split()
        first_char = base_name[0]
        length_bucket = str(len(base_name) // 5)
        first_token = tokens[0] if tokens else "NONE"
        return f"{first_char}_{length_bucket}_{first_token}"

    def get_ratio(self, s1, s2):
        return jellyfish.jaro_winkler_similarity(s1, s2)

    def get_token_sorted_match(self, s1, s2):
        t1 = "".join(sorted(s1.split()))
        t2 = "".join(sorted(s2.split()))
        return t1 == t2

    def process(self, df, column):
        # 1. Clean and Generate Keys
        rows = []
        for i, row in df.iterrows():
            orig = row[column]
            norm, base = self.get_base_name(orig)
            block = self.get_block_key(base)
            rows.append({
                'row_order': i,
                'original_name': orig,
                'normalized_name': norm,
                'base_name': base,
                'block_key': block,
                'cluster_id': i, # Initial cluster is itself
                'confidence': 0.70,
                'reason': 'Isolated or weak match'
            })

        # 2. Blocking & Matching (Union-Find)
        parent = list(range(len(rows)))
        def find(i):
            if parent[i] == i: return i
            parent[i] = find(parent[i])
            return parent[i]

        def union(i, j, ratio, is_token_match):
            root_i = find(i)
            root_j = find(j)
            if root_i != root_j:
                parent[root_i] = root_j
                # Update confidence/reason for the row being merged
                conf, reason = self.calculate_confidence(ratio, is_token_match)
                rows[i]['confidence'] = max(rows[i]['confidence'], conf)
                rows[i]['reason'] = reason

        # Group by blocks
        blocks = defaultdict(list)
        for i, r in enumerate(rows):
            if r['base_name']:
                blocks[r['block_key']].append(i)

        for block_key, indices in blocks.items():
            for i in range(len(indices)):
                for j in range(i + 1, len(indices)):
                    idx1, idx2 = indices[i], indices[j]
                    b1, b2 = rows[idx1]['base_name'], rows[idx2]['base_name']
                    
                    ratio = self.get_ratio(b1, b2)
                    is_token_match = self.get_token_sorted_match(b1, b2)
                    
                    if (is_token_match and ratio >= self.soft_threshold) or (ratio >= self.hard_threshold):
                        union(idx1, idx2, ratio, is_token_match)

        # 3. Finalize Clusters
        clusters = defaultdict(list)
        for i in range(len(rows)):
            root = find(i)
            rows[i]['cluster_id'] = root
            clusters[root].append(i)
            
            # Handle empty base names
            if not rows[i]['base_name']:
                rows[i]['confidence'] = 0.50
                rows[i]['reason'] = "No base name after cleaning; kept as singleton"

        # 4. Optional Web Search Verification for low confidence
        if self.enable_web_search:
            print("Performing web verification for low-confidence clusters...")
            for root, member_indices in clusters.items():
                # Only check if size > 1 and confidence is still relatively low or if it's a singleton we want to verify
                example_idx = member_indices[0]
                if rows[example_idx]['confidence'] < 0.90:
                    base = rows[example_idx]['base_name']
                    if base:
                        web_canonical = self.web_verify(base)
                        if web_canonical and web_canonical != base.upper():
                            # If web search gives a strong hit, we could potentially merge or just update canonical
                            # For now, we update the base_name to help the canonical logic
                            for idx in member_indices:
                                rows[idx]['web_canonical'] = web_canonical
                                rows[idx]['reason'] += f" | Web verified: {web_canonical}"

        # 5. Determine Canonical names
        for root, member_indices in clusters.items():
            # Canonical = shortest, most frequent base_name
            base_names = [rows[idx]['base_name'] for idx in member_indices if rows[idx]['base_name']]
            if not base_names:
                canonical = rows[member_indices[0]]['normalized_name']
            else:
                # Frequency count
                counts = pd.Series(base_names).value_counts()
                max_freq = counts.max()
                candidates = counts[counts == max_freq].index.tolist()
                # Sort by length ascending
                canonical = sorted(candidates, key=len)[0]
            
            size = len(member_indices)
            for idx in member_indices:
                rows[idx]['canonical_name'] = canonical
                rows[idx]['cluster_size'] = size

        return rows

    def calculate_confidence(self, ratio, is_token_match):
        if is_token_match and ratio >= 0.90: return 0.98, "token-sorted match AND ratio >= 0.90"
        if ratio >= 0.90: return 0.95, "ratio >= 0.90"
        if ratio >= 0.85: return 0.88, "ratio >= 0.85"
        return 0.70, "Isolated or weak match"
