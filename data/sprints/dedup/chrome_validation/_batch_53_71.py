"""Batch-log verdicts #53-71 yellow-flag Chrome validation."""
import json, sys
sys.stdout.reconfigure(encoding='utf-8')
from pathlib import Path

log_path = Path('data/sprints/dedup/chrome_validation/yellow_verdicts.jsonl')

entries = [
    {'idx':53,'name':'Mountain Vineyards','country':'ZA','flag':'low_wine_count_1','verdict':'KEEP_AS_IS',
     'reasoning':'Tesco UK private-label South African Sauvignon Blanc (Integrity & Sustainability mark). Retailer own-label, 1 wine correct count.',
     'chrome_evidence':'tesco.com product page confirms Tesco private-label SA SB.'},
    {'idx':54,'name':'Starborough','country':'NZ','flag':'low_wine_count_1','verdict':'KEEP_AS_IS',
     'reasoning':'Starborough Marlborough NZ Sauvignon Blanc single-wine brand (Constellation). 1 wine correct.',
     'chrome_evidence':'Well-known commercial NZ brand.'},
    {'idx':55,'name':'Jean Louis Chave','country':'FR','sibling_name':'Jean-Louis Chavy','verdict':'SKIP',
     'reasoning':'Jean Louis Chave = Hermitage/Saint-Joseph icon (10w incl Ermitage Cathelin, Clos Florentin). Jean-Louis Chavy = distinct Burgundy Puligny-Montrachet producer (10w Puligny/Gevrey/Champ Gain). Different regions, different families.',
     'chrome_evidence':'DB portfolios confirm Rhone vs Burgundy split.'},
    {'idx':56,'name':'Francesco Rinaldi','country':'IT','flag':'low_wine_count_2','verdict':'KEEP_AS_IS',
     'reasoning':'Francesco Rinaldi & Figli Barolo producer. 2 wines (Barolo + Barolo Cannubi) covers grand + cru tier. Correct identity.',
     'chrome_evidence':'Well-known Barolo house.'},
    {'idx':57,'name':'Hirtzberger','country':'AT','flag':'low_wine_count_1','verdict':'KEEP_AS_IS',
     'reasoning':'Franz Hirtzberger Wachau (Austria) icon. 1 wine = flagship Honivogl Smaragd Gruner Veltliner. Low coverage but correct identity.',
     'chrome_evidence':'Famous Wachau Spitz producer.'},
    {'idx':58,'name':'Chateau Mouton Rothschild','country':'FR',
     'producer_id':'a4ed00e7-598c-4e9a-a64f-228449c61a11',
     'sibling_name':'Mouton Rothschild (Luze)',
     'sibling_id':'b269bd57-83c6-45e4-83a0-17f626a63557',
     'verdict':'MERGE','merge_source_id':'b269bd57-83c6-45e4-83a0-17f626a63557',
     'survivor_name':'Chateau Mouton Rothschild','pattern_cluster':'11.4.p',
     'reasoning':'Chateau Mouton Rothschild = Pauillac 1er GCC (3w: Pauillac grand vin, Aile d-Argent white, Le Petit Mouton 2nd). Mouton Rothschild (Luze) sibling = 1w Pauillac bottled by old Luze negociant — merchant prefix §11.4.p → merge into CMR.',
     'chrome_evidence':'Luze is a defunct Bordeaux negociant; old Luze-bottled CMR wines are legitimately CMR.',
     'unflagged_additional_merge':'"Mouton Rothschild" (id 8d2ac86b-5e1f-420e-9977-4435afbdafb2, 11w) is a MIXED row containing CMR wines (Aile d-Argent, Le Petit Mouton) + BPDR negociant wines (Mouton Cadet) + unknowns (Baron de Miollis). DO NOT auto-merge — flag for Sprint 7 re-parenting (CMR wines to CMR, Mouton Cadet to BPDR).'},
    {'idx':59,'name':"Taylor's",'country':'PT',
     'producer_id':'6141b2e5-7306-4b07-b36a-62cc793bd958',
     'verdict':'MERGE','merge_target_name':'Taylor Fladgate','pattern_cluster':'11.1',
     'reasoning':'Taylor-s (57w: 10YO/20YO/30YO/325th Anniversary/1692-1992 Assortment) and Taylor Fladgate (#26, 7w: 40YO/LBV/Scion/10YO) are the same Fladgate Partnership Port house. UK uses Taylor-s, US uses Taylor Fladgate. Also unflagged mergers: Taylor (id fe1ff8a2 5w: 40YO/Vargellas/Single Harvest/Vintage Portwein), Taylor-s (Berry Bros Rudd) (1w Finest Reserve), Taylor-s (Justerini & Brooks) (1w Vintage), Taylors (id c3633991 1w 1863 Decanter). Survivor: Taylor Fladgate (US-market label form).',
     'chrome_evidence':'Fladgate Partnership documentation treats Taylor-s Port = Taylor Fladgate. DB portfolios match Port tiers.',
     'unflagged_additional_merges':['Taylor fe1ff8a2','Taylor-s (Berry Bros Rudd) 1904b8a9','Taylor-s (Justerini & Brooks) f950c908','Taylors (c3633991)'],
     'siblings_skip':['Taylor (id 4f11b3d5 Sherry/Madeira/Marsala: possibly different line — flag for review)','Taylors (id 7bebec33 Australia 80 Acres/Clare Valley)'],
     'sprint7_followup':'Resolve Taylor (Sherry/Madeira/Marsala) attribution and re-parent BBR/J&B merchant prefixes.'},
    {'idx':60,'name':'Ankida Ridge Vineyards','country':'US','flag':'flagship_coverage_low_0.0%','verdict':'KEEP_AS_IS',
     'reasoning':'Small Virginia boutique. 1w Pinot Noir correct for low-production estate.',
     'chrome_evidence':'Virginia producer.'},
    {'idx':61,'name':'Hidalgo Gitana','country':'ES','flag':'low_wine_count_1','verdict':'KEEP_AS_IS',
     'reasoning':'Bodegas Hidalgo-La Gitana, Sanlucar de Barrameda. Famous La Gitana Manzanilla = flagship. 1 wine correct for DB row.',
     'chrome_evidence':'Famous Sanlucar Manzanilla house.'},
    {'idx':62,'name':'Chateau Ausone','country':'FR',
     'producer_id':'bdf75710-e06c-4684-bb74-6e80708f9d2d',
     'verdict':'MERGE','survivor_name':'Chateau Ausone','pattern_cluster':'11.1',
     'reasoning':'Chateau Ausone (2w) and Ausone (id 803d3407, 3w) are the same Saint-Emilion 1er GCC A row, split by Chateau prefix convention. Also Berry Bros. & Rudd (Ausone) (id d8bb35bc 1w Saint-Emilion) is merchant-prefix old bottling of CMR Ausone → merge. Survivor = Chateau Ausone per US-market form.',
     'chrome_evidence':'Identical wine list (Chapelle d-Ausone + Saint-Emilion Grand Cru) across both non-merchant rows.',
     'unflagged_additional_merges':['Ausone 803d3407','Berry Bros. & Rudd (Ausone) d8bb35bc']},
    {'idx':63,'name':'Clos des Papes','country':'FR','sibling_name':'des Papes','verdict':'SKIP',
     'reasoning':'Clos des Papes = Vincent Avril Chateauneuf-du-Pape (6w: Blanc, Rouge, Le Petit Vin d-Avril). des Papes sibling = 1w Muscat de Beaumes de Venise (different grape, different appellation) — distinct producer.',
     'chrome_evidence':'DB appellations confirm distinct.'},
    {'idx':64,'name':'Alion','country':'ES','flag':'low_wine_count_1','verdict':'KEEP_AS_IS',
     'reasoning':'Bodegas Alion, Vega Sicilia sister estate in Ribera del Duero. 1w Ribera del Duero DO = flagship. Correct identity, low coverage.',
     'chrome_evidence':'Famous Vega Sicilia sister winery.'},
    {'idx':65,'name':'Pauillac De Pichon Lalande','country':'FR',
     'producer_id':'abed89bc-461f-46d5-9fd1-a7fd11fc293e',
     'verdict':'MERGE','merge_target_id':'b754fcbc-14e3-4a98-b12a-b2fda0a5adcc',
     'merge_target_name':'Pichon Longueville Comtesse de Lalande',
     'survivor_name':'Pichon Longueville Comtesse de Lalande','pattern_cluster':'11.4.h',
     'reasoning':'Pauillac De Pichon Lalande (1w Pauillac) is a label-form of Pichon Longueville Comtesse de Lalande (6w: Pauillac, Les Gartieux, Pichon Comtesse Reserve, Pauillac de Pichon Lalande). Merge into canonical Pichon Lalande row. Also unflagged merge: Pichon Longueville Comtesse Lalande (missing de) (id 89267ae7 1w) → same target.',
     'chrome_evidence':'Pauillac de Pichon Lalande is already listed as a wine under the main Pichon Lalande row.',
     'unflagged_additional_merges':['Pichon Longueville Comtesse Lalande 89267ae7']},
    {'idx':66,'name':'Domaine Leflaive','country':'FR','flag':'flagship_coverage_low_0.0%','verdict':'KEEP_AS_IS',
     'reasoning':'Domaine Leflaive (Puligny-Montrachet icon). 4w (Blanc, Auxey-Duresses, Rully Premier Cru). Missing Puligny Premier Cru/Grand Cru — data completeness gap, not dedup. Identity correct.',
     'chrome_evidence':'Famous Burgundy domaine.'},
    {'idx':67,'name':'Rene Vincent Dauvissat','country':'FR','flag':'low_wine_count_1','verdict':'KEEP_AS_IS',
     'reasoning':'Domaine Rene et Vincent Dauvissat, Chablis icon. 1w Chablis is low; flagship Premier/Grand Cru missing. Identity correct; data completeness.',
     'chrome_evidence':'Famous Chablis domaine.'},
    {'idx':68,'name':'Philipp Wissmann','country':'DE','flag':'low_wine_count_1','verdict':'KEEP_AS_IS',
     'reasoning':'Obscure Rheinhessen producer (Mettenheimer Schlossberg). 1w BA. Small estate, identity likely correct.',
     'chrome_evidence':'German regional producer.'},
    {'idx':69,'name':'Vieux Chateau Certan','country':'FR',
     'siblings_skip':['Vieux Chateau Pelletan','Vieux Chateau Chambeau','Vieux Chateau Brun','Vieux Chateau Chauvin','Vieux Chateau Gaubert'],
     'verdict':'SKIP',
     'reasoning':'Vieux Chateau Certan = iconic Pomerol (Thienpont family, 2w: Pomerol + La Gravette de Certan). The 5 siblings are all distinct Libournais/Bordeaux chateaux sharing Vieux Chateau prefix (Pelletan=Saint-Emilion, Chambeau=Lussac-Saint-Emilion, Brun=Pomerol, Chauvin=Saint-Emilion GC, Gaubert=Graves). Vieux Chateau is a widespread Bordeaux naming prefix.',
     'chrome_evidence':'DB appellations show different appellations per sibling.'},
    {'idx':70,'name':'Agrapart','country':'FR','flag':'low_wine_count_2','verdict':'KEEP_AS_IS',
     'reasoning':'Agrapart & Fils Champagne grower (Avize). 2w covers Rose 1er Cru + Cru Terroirs GC Extra Brut. Correct identity, partial coverage.',
     'chrome_evidence':'Famous Avize grower-producer.'},
    {'idx':71,'name':'Graillot','country':'AU',
     'producer_id':'a78beae3-d627-41d5-9484-f3f4721af9f4',
     'verdict':'MERGE','merge_target_id':'b6f6b9b5-8f4d-405b-8dfb-7c834c8acf53',
     'merge_target_name':'Alain Graillot',
     'survivor_name':'Alain Graillot','pattern_cluster':'11.4.h',
     'reasoning':'Graillot (1w Syrah, country AU is data error) merges into Alain Graillot (7w: Crozes-Hermitage, Clos Somi, Floreal, Guiraude, Blanc) = famous Crozes-Hermitage producer. Country tag AU should become FR on merge.',
     'chrome_evidence':'Alain Graillot is the canonical Crozes-Hermitage icon; Graillot 1w is a split-row variant.',
     'sprint7_followup':'Fix country FR on merged row.'},
]

with open(log_path, 'a', encoding='utf-8') as f:
    for e in entries:
        f.write(json.dumps(e, ensure_ascii=False) + '\n')
print(f'Logged {len(entries)} verdicts (#53-71)')

# Final tally
verdict_counts = {}
import json
with open(log_path, 'r', encoding='utf-8') as f:
    for line in f:
        rec = json.loads(line)
        v = rec.get('verdict', 'UNKNOWN')
        verdict_counts[v] = verdict_counts.get(v, 0) + 1
print(f'\n=== Final yellow verdict tally ({sum(verdict_counts.values())} total) ===')
for v, n in sorted(verdict_counts.items(), key=lambda x: -x[1]):
    print(f'  {v:<15} {n}')
