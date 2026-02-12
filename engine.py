import re
import pandas as pd
import jellyfish
from collections import defaultdict
from ddgs import DDGS
from concurrent.futures import ThreadPoolExecutor, as_completed
import google.generativeai as genai

class CompanyDedupEngine:
    def __init__(self, settings=None):
        self.settings = settings or {}
        self.hard_threshold = self.settings.get('hard', 0.90)
        self.soft_threshold = self.settings.get('soft', 0.85)
        self.no_subsidiary_fold = self.settings.get('no_subsidiary_fold', False)
        self.enable_web_search = self.settings.get('web_search', False)
        self.enable_enrichment = self.settings.get('enrichment', False)
        
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

    def find_domain(self, name):
        """Use DuckDuckGo to find the official domain of the company."""
        if not name: return ""
        try:
            with DDGS() as ddgs:
                results = list(ddgs.text(f"{name} official website", max_results=3))
                for res in results:
                    url = res.get('href', '')
                    if url:
                        # Extract domain using simple regex
                        match = re.search(r'https?://(?:www\.)?([^/]+)', url)
                        if match:
                            domain = match.group(1).lower()
                            # Basic filtering of non-company domains
                            if not any(x in domain for x in ['linkedin.com', 'wikipedia.org', 'facebook.com', 'twitter.com', 'glassdoor.com']):
                                return domain
        except Exception:
            pass
        return ""

    def classify_industry(self, name):
        """Simple rule-based or search-based industry classification."""
        if not name: return "Unknown"
        
        # Simple keywords mapping
        keywords = {
            'TECHNOLOGY': ['SOFTWARE', 'TECH', 'SaaS', 'COMPUTING', 'DIGITAL'],
            'FINANCE': ['BANK', 'INVESTMENT', 'FINANCIAL', 'CAPITAL', 'INSURANCE'],
            'HEALTHCARE': ['PHARMA', 'HOSPITAL', 'MEDICAL', 'HEALTH', 'BIOTECH'],
            'RETAIL': ['STORE', 'SHOP', 'MARKET', 'COMMERCE'],
            'MANUFACTURING': ['ENGINEERING', 'INDUSTRIAL', 'SYSTEMS', 'ELECTRONICS']
        }
        
        norm_name = name.upper()
        for industry, keys in keywords.items():
            if any(k in norm_name for k in keys):
                return industry

        # Fallback to web search if enabled
        if self.enable_web_search:
            try:
                with DDGS() as ddgs:
                    results = list(ddgs.text(f"{name} industry sector", max_results=1))
                    if results:
                        # Very crude extraction from snippet
                        snippet = results[0]['body'].upper()
                        for industry in keywords.keys():
                            if industry in snippet:
                                return industry
            except Exception:
                pass

        return "Diversified/Other"

    def agentic_research(self, model, name):
        """Use Gemini AI to research a company and provide highly accurate normalization."""
        if not name: return None
        try:
            # Step 1: Search for snippets
            with DDGS() as ddgs:
                results = list(ddgs.text(f"official legal name and website of company {name}", max_results=5))
                snippets = "\n".join([f"- {r.get('body', '')}" for r in results])
            
            if not snippets: return None

            # Step 2: Prompt LLM
            prompt = f"""
            You are an expert data researcher. Your task is to identify the official legal name of a company based on search snippets.
            
            Input Name: {name}
            
            Search Results:
            {snippets}
            
            Analyze the snippets and identify:
            1. The full official legal name (e.g., "Apple Inc." or "Microsoft Corporation").
            2. A brief 1-sentence reason for your choice.
            
            Respond STRICTLY in JSON format:
            {{"name": "OFFICIAL_NAME", "reason": "REASON"}}
            """
            
            response = model.generate_content(prompt)
            # Basic JSON extraction
            text = response.text.strip()
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            
            import json
            data = json.loads(text)
            return data
        except Exception as e:
            print(f"AI Research failed for {name}: {e}")
            return None

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

        # 4. Optional Web Search Verification for low confidence (Parallel)
        if self.enable_web_search:
            print("Performing parallel web verification...")
            to_verify = []
            for root, member_indices in clusters.items():
                example_idx = member_indices[0]
                if rows[example_idx]['confidence'] < 0.90:
                    base = rows[example_idx]['base_name']
                    if base:
                        to_verify.append((root, base, member_indices))
            
            if to_verify:
                # Decide if we use AI or standard verification
                api_key = self.settings.get('gemini_api_key')
                use_ai = self.settings.get('agentic_mode', False) and api_key
                
                if use_ai:
                    genai.configure(api_key=api_key)
                    model = genai.GenerativeModel('gemini-1.5-flash')
                
                with ThreadPoolExecutor(max_workers=5) as executor:
                    if use_ai:
                        future_to_cluster = {executor.submit(self.agentic_research, model, item[1]): item for item in to_verify}
                    else:
                        future_to_cluster = {executor.submit(self.web_verify, item[1]): item for item in to_verify}
                        
                    for future in as_completed(future_to_cluster):
                        root, base, member_indices = future_to_cluster[future]
                        try:
                            result = future.result()
                            if isinstance(result, dict): # AI Result
                                web_canonical = result.get('name')
                                ai_reason = result.get('reason', '')
                                if web_canonical and web_canonical.upper() != base.upper():
                                    for idx in member_indices:
                                        rows[idx]['web_canonical'] = web_canonical
                                        rows[idx]['reason'] += f" | AI Verified: {ai_reason}"
                            else: # Standard result
                                web_canonical = result
                                if web_canonical and web_canonical != base.upper():
                                    for idx in member_indices:
                                        rows[idx]['web_canonical'] = web_canonical
                                        rows[idx]['reason'] += f" | Web verified: {web_canonical}"
                        except Exception:
                            pass

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

        # 6. Data Enrichment (Parallel)
        if self.enable_enrichment:
            print("Performing parallel data enrichment...")
            # For efficiency, only enrich canonical names once per cluster
            canonical_list = list(set(rows[idx]['canonical_name'] for idx in range(len(rows))))
            enriched_data = {}
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                # Submit domain finding
                domain_futures = {executor.submit(self.find_domain, name): name for name in canonical_list}
                # Submit industry classification
                industry_futures = {executor.submit(self.classify_industry, name): name for name in canonical_list}
                
                # Collect domains
                for future in as_completed(domain_futures):
                    name = domain_futures[future]
                    enriched_data.setdefault(name, {})['website'] = future.result()
                
                # Collect industries
                for future in as_completed(industry_futures):
                    name = industry_futures[future]
                    enriched_data.setdefault(name, {})['industry'] = future.result()

            # Apply to all rows
            for i in range(len(rows)):
                can = rows[i]['canonical_name']
                rows[i]['website'] = enriched_data.get(can, {}).get('website', "")
                rows[i]['industry'] = enriched_data.get(can, {}).get('industry', "Unknown")

        return rows

    def calculate_confidence(self, ratio, is_token_match):
        if is_token_match and ratio >= 0.90: return 0.98, "token-sorted match AND ratio >= 0.90"
        if ratio >= 0.90: return 0.95, "ratio >= 0.90"
        if ratio >= 0.85: return 0.88, "ratio >= 0.85"
        return 0.70, "Isolated or weak match"
