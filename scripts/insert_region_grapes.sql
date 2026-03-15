-- ============================================================================
-- Region Grapes INSERT statements
-- Generated from authoritative sources (see notes column for attribution)
-- All association_type = 'typical' (these are plantings, not regulatory requirements)
-- ============================================================================

-- ============================================================================
-- AUSTRALIA L2 REGIONS
-- Source: Wine Australia regional profiles (wineaustralia.com)
-- ============================================================================

-- Barossa Valley (d14e0395-ee92-48e1-a2ce-70bdc3b2f531)
-- Source: Wine Australia "The Barossa Valley" profile — Shiraz 50% of plantings,
-- plus Grenache, Cabernet Sauvignon, Mourvèdre (Mataro), Riesling, Chardonnay
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('d14e0395-ee92-48e1-a2ce-70bdc3b2f531', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: Wine Australia Barossa Valley profile. ~50% of plantings.'),
('d14e0395-ee92-48e1-a2ce-70bdc3b2f531', '85b5249b-c8c2-4852-bb71-3f62eebf9d2a', 'typical', 'Source: Wine Australia Barossa Valley profile. Heritage variety, old vine GSM blends.'),
('d14e0395-ee92-48e1-a2ce-70bdc3b2f531', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Wine Australia Barossa Valley profile. Major red variety.'),
('d14e0395-ee92-48e1-a2ce-70bdc3b2f531', 'e0b7b143-5ac7-4bed-937d-732a9270b67e', 'typical', 'Source: Wine Australia Barossa Valley profile. GSM blend component (Mataro).'),
('d14e0395-ee92-48e1-a2ce-70bdc3b2f531', '5ea633a0-a830-4a67-83dd-536108358c41', 'typical', 'Source: Wine Australia Barossa Valley profile. Signature white variety.'),
('d14e0395-ee92-48e1-a2ce-70bdc3b2f531', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Wine Australia Barossa Valley profile.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- McLaren Vale (17ca111d-16cc-4164-8194-822b2b0f9276)
-- Source: Wine Australia McLaren Vale profile — Shiraz ~50% of crush,
-- plus Grenache (old vines, GSM), Cabernet Sauvignon, Chardonnay (most planted white)
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('17ca111d-16cc-4164-8194-822b2b0f9276', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: Wine Australia McLaren Vale profile. ~50% of annual crush.'),
('17ca111d-16cc-4164-8194-822b2b0f9276', '85b5249b-c8c2-4852-bb71-3f62eebf9d2a', 'typical', 'Source: Wine Australia McLaren Vale profile. Old vines dating to 1800s, GSM backbone.'),
('17ca111d-16cc-4164-8194-822b2b0f9276', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Wine Australia McLaren Vale profile. Major red variety.'),
('17ca111d-16cc-4164-8194-822b2b0f9276', 'e0b7b143-5ac7-4bed-937d-732a9270b67e', 'typical', 'Source: Wine Australia McLaren Vale profile. GSM blend component.'),
('17ca111d-16cc-4164-8194-822b2b0f9276', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Wine Australia McLaren Vale profile. Most planted white variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Clare Valley (e40e99f9-5302-439d-9023-9f85413ebd37)
-- Source: Wine Australia Clare Valley profile — Riesling synonymous with region,
-- plus Shiraz and Cabernet Sauvignon
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('e40e99f9-5302-439d-9023-9f85413ebd37', '5ea633a0-a830-4a67-83dd-536108358c41', 'typical', 'Source: Wine Australia Clare Valley profile. Synonymous with the region.'),
('e40e99f9-5302-439d-9023-9f85413ebd37', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: Wine Australia Clare Valley profile. Full-flavoured blackberry and spice.'),
('e40e99f9-5302-439d-9023-9f85413ebd37', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Wine Australia Clare Valley profile. Major red variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Eden Valley (7e030f96-a987-4f04-8371-a3e85c538ad0)
-- Source: Wine Australia Eden Valley profile — Riesling reigns supreme,
-- plus Shiraz (most important red), Chardonnay, Cabernet Sauvignon
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('7e030f96-a987-4f04-8371-a3e85c538ad0', '5ea633a0-a830-4a67-83dd-536108358c41', 'typical', 'Source: Wine Australia Eden Valley profile. Reigns supreme; signature variety.'),
('7e030f96-a987-4f04-8371-a3e85c538ad0', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: Wine Australia Eden Valley profile. Most important red grape.'),
('7e030f96-a987-4f04-8371-a3e85c538ad0', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Wine Australia Eden Valley profile.'),
('7e030f96-a987-4f04-8371-a3e85c538ad0', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Wine Australia Eden Valley profile.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Coonawarra (289fb4bd-1bc9-4983-a87a-ccc595b53448)
-- Source: Wine Australia Coonawarra profile — pre-eminent Cabernet Sauvignon,
-- plus Shiraz (historically dominant), Merlot, Chardonnay
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('289fb4bd-1bc9-4983-a87a-ccc595b53448', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Wine Australia Coonawarra profile. Pre-eminent producer of Cabernet Sauvignon in Australia.'),
('289fb4bd-1bc9-4983-a87a-ccc595b53448', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: Wine Australia Coonawarra profile. Long history, main variety until 1950s.'),
('289fb4bd-1bc9-4983-a87a-ccc595b53448', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: Wine Australia Coonawarra profile.'),
('289fb4bd-1bc9-4983-a87a-ccc595b53448', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Wine Australia Coonawarra profile.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Adelaide Hills (31b41119-20c4-498b-a819-7be02d3fa7bf)
-- Source: Wine Australia Adelaide Hills profile — Sauvignon Blanc benchmark,
-- plus Pinot Noir, Chardonnay, Shiraz
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('31b41119-20c4-498b-a819-7be02d3fa7bf', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: Wine Australia Adelaide Hills profile. Benchmark region for Australian Sauvignon Blanc.'),
('31b41119-20c4-498b-a819-7be02d3fa7bf', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: Wine Australia Adelaide Hills profile. Major variety.'),
('31b41119-20c4-498b-a819-7be02d3fa7bf', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Wine Australia Adelaide Hills profile.'),
('31b41119-20c4-498b-a819-7be02d3fa7bf', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: Wine Australia Adelaide Hills profile. Cool-climate style.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Margaret River (60c4843c-9eac-438f-bcef-a9ab7668f461)
-- Source: Wine Australia Margaret River profile — Cabernet Sauvignon, Chardonnay,
-- Sauvignon Blanc, Sémillon
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('60c4843c-9eac-438f-bcef-a9ab7668f461', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Wine Australia Margaret River profile. Top red variety.'),
('60c4843c-9eac-438f-bcef-a9ab7668f461', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Wine Australia Margaret River profile. World-class Chardonnay (Gin Gin clone).'),
('60c4843c-9eac-438f-bcef-a9ab7668f461', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: Wine Australia Margaret River profile. Major white, often SBS blends.'),
('60c4843c-9eac-438f-bcef-a9ab7668f461', '8d32c133-577b-4ac8-8e4c-91637382a1b3', 'typical', 'Source: Wine Australia Margaret River profile. SBS blend component.'),
('60c4843c-9eac-438f-bcef-a9ab7668f461', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: Wine Australia Margaret River profile. Bordeaux-style red blends.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Yarra Valley (6f58ac9d-8899-4dd7-870d-4d7efd212639)
-- Source: Wine Australia Yarra Valley profile — Victoria's first wine district,
-- Pinot Noir, Chardonnay, Shiraz (cool-climate), Cabernet Sauvignon
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('6f58ac9d-8899-4dd7-870d-4d7efd212639', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: Wine Australia Yarra Valley profile. Premier cool-climate Pinot Noir.'),
('6f58ac9d-8899-4dd7-870d-4d7efd212639', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Wine Australia Yarra Valley profile. Major variety.'),
('6f58ac9d-8899-4dd7-870d-4d7efd212639', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: Wine Australia Yarra Valley profile. Cool-climate style.'),
('6f58ac9d-8899-4dd7-870d-4d7efd212639', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Wine Australia Yarra Valley profile.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Hunter Valley (b98e6e1f-dcd0-47b1-b28e-66445352fec1)
-- Source: Wine Australia Hunter Valley profile — Shiraz outstanding red,
-- Sémillon is signature white (unique Hunter style), plus Chardonnay, Verdelho
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('b98e6e1f-dcd0-47b1-b28e-66445352fec1', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: Wine Australia Hunter Valley profile. Outstanding red grape of the region.'),
('b98e6e1f-dcd0-47b1-b28e-66445352fec1', '8d32c133-577b-4ac8-8e4c-91637382a1b3', 'typical', 'Source: Wine Australia Hunter Valley profile. Iconic Hunter Valley Semillon, unique long-lived style.'),
('b98e6e1f-dcd0-47b1-b28e-66445352fec1', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Wine Australia Hunter Valley profile.'),
('b98e6e1f-dcd0-47b1-b28e-66445352fec1', '7175e07d-55dd-43c4-8e77-901b0fbbab7f', 'typical', 'Source: Wine Australia Hunter Valley profile. Verdelho is established white variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Mornington Peninsula (bfad352a-e620-4105-8936-a96ab2149c78)
-- Source: Wine Australia Mornington Peninsula profile — "Pinot Paradise",
-- plus Chardonnay, Pinot Gris, Shiraz (cool-climate)
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('bfad352a-e620-4105-8936-a96ab2149c78', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: Wine Australia Mornington Peninsula profile. "Pinot Paradise" — dominant variety.'),
('bfad352a-e620-4105-8936-a96ab2149c78', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Wine Australia Mornington Peninsula profile.'),
('bfad352a-e620-4105-8936-a96ab2149c78', '55b05ae2-de32-4951-82f4-be5703e5b9c7', 'typical', 'Source: Wine Australia Mornington Peninsula profile.'),
('bfad352a-e620-4105-8936-a96ab2149c78', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: Wine Australia Mornington Peninsula profile. Cool-climate Shiraz.')
ON CONFLICT (region_id, grape_id) DO NOTHING;


-- ============================================================================
-- NEW ZEALAND L2 REGIONS
-- Source: New Zealand Winegrowers (nzwine.com) regional profiles
-- ============================================================================

-- Marlborough (1ed5b923-e017-4d81-85ed-7af5168ccd4b)
-- Source: NZ Winegrowers — 82% Sauvignon Blanc, 75% Pinot Noir (of reds)
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('1ed5b923-e017-4d81-85ed-7af5168ccd4b', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: NZ Winegrowers Marlborough profile. 82% of planted white varieties.'),
('1ed5b923-e017-4d81-85ed-7af5168ccd4b', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: NZ Winegrowers Marlborough profile. 75% of planted red varieties.'),
('1ed5b923-e017-4d81-85ed-7af5168ccd4b', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: NZ Winegrowers Marlborough profile.'),
('1ed5b923-e017-4d81-85ed-7af5168ccd4b', '55b05ae2-de32-4951-82f4-be5703e5b9c7', 'typical', 'Source: NZ Winegrowers Marlborough profile.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Central Otago (42245b77-f570-475b-b1e0-0c2313a98128)
-- Source: NZ Winegrowers — predominantly Pinot Noir, plus Pinot Gris, Riesling, Chardonnay
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('42245b77-f570-475b-b1e0-0c2313a98128', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: NZ Winegrowers Central Otago profile. Dominant variety, world-class Pinot Noir.'),
('42245b77-f570-475b-b1e0-0c2313a98128', '55b05ae2-de32-4951-82f4-be5703e5b9c7', 'typical', 'Source: NZ Winegrowers Central Otago profile.'),
('42245b77-f570-475b-b1e0-0c2313a98128', '5ea633a0-a830-4a67-83dd-536108358c41', 'typical', 'Source: NZ Winegrowers Central Otago profile.'),
('42245b77-f570-475b-b1e0-0c2313a98128', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: NZ Winegrowers Central Otago profile.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Hawke's Bay (0dbc2fa9-c798-4886-8258-49d2bb9d3642)
-- Source: NZ Winegrowers — Bordeaux reds (Merlot, Cabernet Sauvignon), Chardonnay, Syrah
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('0dbc2fa9-c798-4886-8258-49d2bb9d3642', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: NZ Winegrowers Hawke''s Bay profile. Key red variety, esp. Gimblett Gravels.'),
('0dbc2fa9-c798-4886-8258-49d2bb9d3642', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: NZ Winegrowers Hawke''s Bay profile. Bordeaux blend component.'),
('0dbc2fa9-c798-4886-8258-49d2bb9d3642', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: NZ Winegrowers Hawke''s Bay profile. Premium Chardonnay.'),
('0dbc2fa9-c798-4886-8258-49d2bb9d3642', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: NZ Winegrowers Hawke''s Bay profile. Impressive Syrah.'),
('0dbc2fa9-c798-4886-8258-49d2bb9d3642', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: NZ Winegrowers Hawke''s Bay profile.'),
('0dbc2fa9-c798-4886-8258-49d2bb9d3642', '55b05ae2-de32-4951-82f4-be5703e5b9c7', 'typical', 'Source: NZ Winegrowers Hawke''s Bay profile.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Martinborough (c845e9e8-3279-4c94-a087-6af57705dc2c)
-- Source: NZ Winegrowers Wairarapa/Martinborough profile — Pinot Noir, Sauvignon Blanc,
-- aromatics, Syrah, Pinot Gris
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('c845e9e8-3279-4c94-a087-6af57705dc2c', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: NZ Winegrowers Wairarapa profile. Acclaimed Pinot Noir.'),
('c845e9e8-3279-4c94-a087-6af57705dc2c', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: NZ Winegrowers Wairarapa profile. Vivid Sauvignon Blanc.'),
('c845e9e8-3279-4c94-a087-6af57705dc2c', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: NZ Winegrowers Wairarapa profile. Elegant Syrah.'),
('c845e9e8-3279-4c94-a087-6af57705dc2c', '55b05ae2-de32-4951-82f4-be5703e5b9c7', 'typical', 'Source: NZ Winegrowers Wairarapa profile.')
ON CONFLICT (region_id, grape_id) DO NOTHING;


-- ============================================================================
-- UNITED STATES L2 REGIONS
-- Sources: USDA NASS California Grape Acreage Reports, Napa County Crop Report,
-- Sonoma County Crop Report, Oregon Wine Board, Washington State Wine Commission,
-- Finger Lakes Wine Alliance, Lodi Winegrape Commission
-- ============================================================================

-- Napa Valley (a0c0198d-8133-459c-ba2f-8ede0d628e40)
-- Source: USDA NASS / Napa County 2024 Crop Report — Cabernet Sauvignon 24,839 acres,
-- Chardonnay 5,662 acres, Merlot 3,515 acres, Cabernet Franc 1,270 acres
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('a0c0198d-8133-459c-ba2f-8ede0d628e40', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Napa County 2024 Crop Report. 24,839 acres — dominant variety.'),
('a0c0198d-8133-459c-ba2f-8ede0d628e40', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Napa County 2024 Crop Report. 5,662 acres — top white variety.'),
('a0c0198d-8133-459c-ba2f-8ede0d628e40', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: Napa County 2024 Crop Report. 3,515 acres.'),
('a0c0198d-8133-459c-ba2f-8ede0d628e40', '839e54a3-75b7-451a-ae05-2bc5b5759d53', 'typical', 'Source: Napa County 2024 Crop Report. 1,270 acres. Bordeaux blend component.'),
('a0c0198d-8133-459c-ba2f-8ede0d628e40', '9c642231-dd8e-4f9d-90fb-ef00ff27ece0', 'typical', 'Source: Napa County 2024 Crop Report. Bordeaux blend component.'),
('a0c0198d-8133-459c-ba2f-8ede0d628e40', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: Napa County 2024 Crop Report.'),
('a0c0198d-8133-459c-ba2f-8ede0d628e40', 'bd1973df-8960-4da3-a506-fa4af28f694b', 'typical', 'Source: Napa County 2024 Crop Report. Bordeaux blend component.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Sonoma County (b48c095e-31e7-4544-940e-c79b07c6af45)
-- Source: Sonoma County Tourism / Sonoma County Winegrowers — 7 varieties >90% of acreage:
-- Chardonnay 15,500ac, Pinot Noir 13,000ac, Cab Sauv 12,700ac, Zinfandel 4,760ac,
-- Merlot 4,200ac, Sauv Blanc 2,600ac, Syrah 1,380ac
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('b48c095e-31e7-4544-940e-c79b07c6af45', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Sonoma County Wine Facts. 15,500 acres — top variety.'),
('b48c095e-31e7-4544-940e-c79b07c6af45', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: Sonoma County Wine Facts. 13,000 acres.'),
('b48c095e-31e7-4544-940e-c79b07c6af45', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Sonoma County Wine Facts. 12,700 acres.'),
('b48c095e-31e7-4544-940e-c79b07c6af45', '063cb5a4-16e3-4c8a-aec4-5dcb021a7b19', 'typical', 'Source: Sonoma County Wine Facts. 4,760 acres. Heritage variety.'),
('b48c095e-31e7-4544-940e-c79b07c6af45', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: Sonoma County Wine Facts. 4,200 acres.'),
('b48c095e-31e7-4544-940e-c79b07c6af45', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: Sonoma County Wine Facts. 2,600 acres.'),
('b48c095e-31e7-4544-940e-c79b07c6af45', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: Sonoma County Wine Facts. 1,380 acres.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Willamette Valley (7e8a83f8-1fdf-48b5-92b7-b2056dd8f52c)
-- Source: Oregon Wine Board / Willamette Valley Wineries — Pinot Noir 70%,
-- Pinot Gris 16%, Chardonnay 7.5%, plus Riesling, Pinot Blanc
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('7e8a83f8-1fdf-48b5-92b7-b2056dd8f52c', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: Oregon Wine Board / Willamette Valley Wineries. 70% of plantings — flagship variety.'),
('7e8a83f8-1fdf-48b5-92b7-b2056dd8f52c', '55b05ae2-de32-4951-82f4-be5703e5b9c7', 'typical', 'Source: Oregon Wine Board. 16% of plantings.'),
('7e8a83f8-1fdf-48b5-92b7-b2056dd8f52c', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Oregon Wine Board. 7.5% of plantings.'),
('7e8a83f8-1fdf-48b5-92b7-b2056dd8f52c', '5ea633a0-a830-4a67-83dd-536108358c41', 'typical', 'Source: Oregon Wine Board. Cool-climate Riesling.'),
('7e8a83f8-1fdf-48b5-92b7-b2056dd8f52c', '6e406be9-17ce-421f-81dc-046ce92dfe58', 'typical', 'Source: Oregon Wine Board. Pinot Blanc.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Columbia Valley (0a37b509-007b-4ca3-9a8d-2eaef285b69f)
-- Source: Washington State Wine Commission — Cabernet Sauvignon most planted,
-- then Merlot, Chardonnay, Riesling, Syrah. ~99% of WA state vineyards.
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('0a37b509-007b-4ca3-9a8d-2eaef285b69f', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Washington State Wine Commission. Most planted variety.'),
('0a37b509-007b-4ca3-9a8d-2eaef285b69f', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: Washington State Wine Commission. Second most planted.'),
('0a37b509-007b-4ca3-9a8d-2eaef285b69f', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Washington State Wine Commission.'),
('0a37b509-007b-4ca3-9a8d-2eaef285b69f', '5ea633a0-a830-4a67-83dd-536108358c41', 'typical', 'Source: Washington State Wine Commission. Major white variety.'),
('0a37b509-007b-4ca3-9a8d-2eaef285b69f', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: Washington State Wine Commission.'),
('0a37b509-007b-4ca3-9a8d-2eaef285b69f', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: Washington State Wine Commission.'),
('0a37b509-007b-4ca3-9a8d-2eaef285b69f', '55b05ae2-de32-4951-82f4-be5703e5b9c7', 'typical', 'Source: Washington State Wine Commission.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Santa Barbara County (95b40a3f-1867-4465-acc9-83372258243c)
-- Source: USDA NASS / Santa Barbara County — Chardonnay 27.7%, Pinot Noir 36%,
-- Syrah 8.2%, plus Viognier, Grenache, Sauvignon Blanc
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('95b40a3f-1867-4465-acc9-83372258243c', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: USDA NASS / SAMsARA Wine. ~36% of plantings.'),
('95b40a3f-1867-4465-acc9-83372258243c', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: USDA NASS. ~28% of plantings — most widely planted.'),
('95b40a3f-1867-4465-acc9-83372258243c', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: USDA NASS. ~8% of plantings. Northern Rhone-style in cool sites.'),
('95b40a3f-1867-4465-acc9-83372258243c', '72f81853-b586-4173-a922-fca8f75d2029', 'typical', 'Source: SAMsARA Wine / Wine Folly Santa Barbara profile. Rhone white variety.'),
('95b40a3f-1867-4465-acc9-83372258243c', '85b5249b-c8c2-4852-bb71-3f62eebf9d2a', 'typical', 'Source: Wine Folly Santa Barbara profile. Rhone red variety, esp. Ballard Canyon.'),
('95b40a3f-1867-4465-acc9-83372258243c', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: USDA NASS Santa Barbara County.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Finger Lakes (036ecf2d-d570-4ab4-9a07-09bba6d3ccc7)
-- Source: Finger Lakes Wine Alliance / NYWGF — Riesling ~46% of vinifera,
-- Cabernet Franc most planted red vinifera (638 acres statewide),
-- plus Chardonnay, Pinot Noir, Gewürztraminer
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('036ecf2d-d570-4ab4-9a07-09bba6d3ccc7', '5ea633a0-a830-4a67-83dd-536108358c41', 'typical', 'Source: Finger Lakes Wine Alliance / NYWGF. ~46% of vinifera — 950+ acres. Defines modern Finger Lakes.'),
('036ecf2d-d570-4ab4-9a07-09bba6d3ccc7', '839e54a3-75b7-451a-ae05-2bc5b5759d53', 'typical', 'Source: Finger Lakes Wine Alliance. Most widely planted red vinifera in NY.'),
('036ecf2d-d570-4ab4-9a07-09bba6d3ccc7', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Finger Lakes Wine Alliance.'),
('036ecf2d-d570-4ab4-9a07-09bba6d3ccc7', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: Finger Lakes Wine Alliance.'),
('036ecf2d-d570-4ab4-9a07-09bba6d3ccc7', 'fca7e900-49ec-44e2-8ee7-153f95ef717f', 'typical', 'Source: Finger Lakes Wine Alliance. Aromatic variety suited to cool climate.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Lodi (a9bbffb6-9827-4ed7-9043-5a989dfa8c4d)
-- Source: Lodi Winegrape Commission — Zinfandel ~40% of CA total,
-- plus Cabernet Sauvignon, Merlot, Chardonnay, Sauvignon Blanc, Petite Sirah (Durif)
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('a9bbffb6-9827-4ed7-9043-5a989dfa8c4d', '063cb5a4-16e3-4c8a-aec4-5dcb021a7b19', 'typical', 'Source: Lodi Winegrape Commission. ~40% of CA Zinfandel. Old vine heritage, "Zinfandel Capital of the World."'),
('a9bbffb6-9827-4ed7-9043-5a989dfa8c4d', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Lodi Winegrape Commission.'),
('a9bbffb6-9827-4ed7-9043-5a989dfa8c4d', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: Lodi Winegrape Commission.'),
('a9bbffb6-9827-4ed7-9043-5a989dfa8c4d', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Lodi Winegrape Commission.'),
('a9bbffb6-9827-4ed7-9043-5a989dfa8c4d', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: Lodi Winegrape Commission.'),
('a9bbffb6-9827-4ed7-9043-5a989dfa8c4d', 'be0adc0c-526d-4508-8235-1c888047f98a', 'typical', 'Source: Lodi Winegrape Commission. Petite Sirah (Durif) — significant Lodi variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;


-- ============================================================================
-- SOUTH AFRICA L2 REGIONS
-- Sources: SAWIS 2021 Vineyard Status Report, WOSA (wosa.co.za),
-- Stellenbosch Wine Routes
-- ============================================================================

-- Stellenbosch (edc5b88d-1420-40f5-a705-ef5c989a7911)
-- Source: SAWIS 2021 — Cab Sauv 5,980ac, Shiraz 3,829ac, Merlot 3,513ac,
-- Pinotage 2,606ac, Sauv Blanc 3,913ac, Chenin Blanc 3,164ac, Chardonnay 2,360ac
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('edc5b88d-1420-40f5-a705-ef5c989a7911', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: SAWIS 2021. ~5,980 acres — most planted red variety.'),
('edc5b88d-1420-40f5-a705-ef5c989a7911', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: SAWIS 2021. ~3,829 acres.'),
('edc5b88d-1420-40f5-a705-ef5c989a7911', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: SAWIS 2021. ~3,513 acres.'),
('edc5b88d-1420-40f5-a705-ef5c989a7911', '87d3bab7-8e4c-4cbe-9149-3b8105fa64aa', 'typical', 'Source: SAWIS 2021. ~2,606 acres.'),
('edc5b88d-1420-40f5-a705-ef5c989a7911', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: SAWIS 2021. ~3,913 acres — most planted white variety.'),
('edc5b88d-1420-40f5-a705-ef5c989a7911', '20c9c863-be98-4753-a901-e6715eef54ae', 'typical', 'Source: SAWIS 2021. ~3,164 acres.'),
('edc5b88d-1420-40f5-a705-ef5c989a7911', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: SAWIS 2021. ~2,360 acres.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Swartland (3bcf1931-a300-4bc2-bce3-4dfea655b38c)
-- Source: SAWIS 2021 — Chenin Blanc ~5,528ac (dominant by landslide),
-- Shiraz, Cabernet Sauvignon, Pinotage ~2,700-3,800ac each
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('3bcf1931-a300-4bc2-bce3-4dfea655b38c', '20c9c863-be98-4753-a901-e6715eef54ae', 'typical', 'Source: SAWIS 2021. ~5,528 acres — dominant variety by a landslide.'),
('3bcf1931-a300-4bc2-bce3-4dfea655b38c', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: SAWIS 2021. Major red variety.'),
('3bcf1931-a300-4bc2-bce3-4dfea655b38c', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: SAWIS 2021.'),
('3bcf1931-a300-4bc2-bce3-4dfea655b38c', '87d3bab7-8e4c-4cbe-9149-3b8105fa64aa', 'typical', 'Source: SAWIS 2021.'),
('3bcf1931-a300-4bc2-bce3-4dfea655b38c', '85b5249b-c8c2-4852-bb71-3f62eebf9d2a', 'typical', 'Source: WOSA / Swartland profile. Rhone-style blends increasingly important.'),
('3bcf1931-a300-4bc2-bce3-4dfea655b38c', 'e0b7b143-5ac7-4bed-937d-732a9270b67e', 'typical', 'Source: WOSA / Swartland profile. Rhone-style blends.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Paarl (320e90cc-0f89-45b0-a1f9-435b40febc12)
-- Source: SAWIS 2021 / WOSA — Cabernet Sauvignon, Pinotage, Shiraz,
-- Chardonnay, Chenin Blanc
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('320e90cc-0f89-45b0-a1f9-435b40febc12', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: SAWIS 2021 / WOSA. Major red variety.'),
('320e90cc-0f89-45b0-a1f9-435b40febc12', '87d3bab7-8e4c-4cbe-9149-3b8105fa64aa', 'typical', 'Source: SAWIS 2021 / WOSA. Best potential variety.'),
('320e90cc-0f89-45b0-a1f9-435b40febc12', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: SAWIS 2021 / WOSA.'),
('320e90cc-0f89-45b0-a1f9-435b40febc12', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: SAWIS 2021 / WOSA.'),
('320e90cc-0f89-45b0-a1f9-435b40febc12', '20c9c863-be98-4753-a901-e6715eef54ae', 'typical', 'Source: SAWIS 2021 / WOSA.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Robertson (e4902800-ce09-4b27-a7fe-9877106399ff)
-- Source: WOSA — traditionally white wine territory, Chardonnay, Sauvignon Blanc,
-- plus Shiraz and Cabernet Sauvignon reds
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('e4902800-ce09-4b27-a7fe-9877106399ff', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: WOSA Robertson profile. Traditional white wine territory, known for Chardonnay.'),
('e4902800-ce09-4b27-a7fe-9877106399ff', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: WOSA Robertson profile. Growing quality reputation.'),
('e4902800-ce09-4b27-a7fe-9877106399ff', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: WOSA Robertson profile. Finest reds.'),
('e4902800-ce09-4b27-a7fe-9877106399ff', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: WOSA Robertson profile.'),
('e4902800-ce09-4b27-a7fe-9877106399ff', '20c9c863-be98-4753-a901-e6715eef54ae', 'typical', 'Source: WOSA Robertson profile. Widely planted.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Walker Bay (38674018-6d3c-409b-82cc-2dd9c5d55f76)
-- Source: WOSA / WineTourism.com — Pinot Noir and Chardonnay (Burgundy varieties),
-- Sauvignon Blanc, Merlot, Syrah
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('38674018-6d3c-409b-82cc-2dd9c5d55f76', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: WOSA Walker Bay profile. Well-known for Pinot Noir.'),
('38674018-6d3c-409b-82cc-2dd9c5d55f76', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: WOSA Walker Bay profile. Burgundy variety alongside Pinot Noir.'),
('38674018-6d3c-409b-82cc-2dd9c5d55f76', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: WOSA Walker Bay profile. Crisp, mineral Sauvignon Blanc.'),
('38674018-6d3c-409b-82cc-2dd9c5d55f76', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: WOSA Walker Bay profile.'),
('38674018-6d3c-409b-82cc-2dd9c5d55f76', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: WOSA Walker Bay profile.')
ON CONFLICT (region_id, grape_id) DO NOTHING;


-- ============================================================================
-- FRANCE L1/L2 REGIONS
-- Sources: Wikipedia (appellation regulations), INAO, Wine Folly, Wine-Searcher,
-- Wine Scholar Guild, official appellation rules
-- ============================================================================

-- Beaujolais (209163ac-0501-419d-b66f-e6e7bc652c3d)
-- Source: Wikipedia Beaujolais / INAO — 96% Gamay, 4% Chardonnay
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('209163ac-0501-419d-b66f-e6e7bc652c3d', 'c4636465-5fef-4cee-865b-0d3bafd6d224', 'typical', 'Source: INAO / Wikipedia Beaujolais. 96% of plantings — defines the region.'),
('209163ac-0501-419d-b66f-e6e7bc652c3d', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: INAO / Wikipedia Beaujolais. ~4% of plantings.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Jura (51024d29-2dd5-4fe7-b888-ca25cff2359b)
-- Source: Wikipedia Jura wine / Wine-Searcher — Chardonnay 48%, Poulsard 18%,
-- Savagnin 17%, Pinot Noir 11%, Trousseau 5%
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('51024d29-2dd5-4fe7-b888-ca25cff2359b', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Jura wine authorities. 48% of plantings.'),
('51024d29-2dd5-4fe7-b888-ca25cff2359b', 'fdadfb80-1694-4bd8-a7f5-0867afb191de', 'typical', 'Source: Jura wine authorities. 18% of plantings. Indigenous variety (Ploussard).'),
('51024d29-2dd5-4fe7-b888-ca25cff2359b', 'f00c5845-cd4d-4073-8c2d-802bd9c8510b', 'typical', 'Source: Jura wine authorities. 17% of plantings. Vin Jaune grape.'),
('51024d29-2dd5-4fe7-b888-ca25cff2359b', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: Jura wine authorities. 11% of plantings.'),
('51024d29-2dd5-4fe7-b888-ca25cff2359b', '00e71526-fca9-489f-8889-447921083b39', 'typical', 'Source: Jura wine authorities. 5% of plantings. Indigenous variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Savoie (ade910f4-b879-485a-96e3-9edc2e8b2bf1)
-- Source: Wine Scholar Guild / NZ Wine / Wikipedia Vin de Savoie —
-- Jacquère (most planted white), Altesse (Roussette), Mondeuse Noire, Gamay, Pinot Noir
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('ade910f4-b879-485a-96e3-9edc2e8b2bf1', 'c83557bb-fb1f-4999-b1ab-36cd797acc59', 'typical', 'Source: Wine Scholar Guild Savoie profile. Most widely planted variety.'),
('ade910f4-b879-485a-96e3-9edc2e8b2bf1', '6537c420-8b56-49af-9948-bacc15d2cc26', 'typical', 'Source: Wine Scholar Guild Savoie profile. Noble variety (Roussette), age-worthy.'),
('ade910f4-b879-485a-96e3-9edc2e8b2bf1', '92288c6d-919c-4a08-ab2a-5d955c15845d', 'typical', 'Source: Wine Scholar Guild Savoie profile. Indigenous red variety.'),
('ade910f4-b879-485a-96e3-9edc2e8b2bf1', 'c4636465-5fef-4cee-865b-0d3bafd6d224', 'typical', 'Source: Wine Scholar Guild Savoie profile. Important red variety.'),
('ade910f4-b879-485a-96e3-9edc2e8b2bf1', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: Wine Scholar Guild Savoie profile.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Northern Rhone (46cbe27e-8952-49ad-9d9f-78eb0abf3c2e)
-- Source: Wikipedia Rhone wine / Ridge Vineyards guide — Syrah (only red permitted),
-- Viognier, Marsanne, Roussanne
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('46cbe27e-8952-49ad-9d9f-78eb0abf3c2e', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: INAO / Rhone wine regulations. Only red grape permitted in Northern Rhone reds.'),
('46cbe27e-8952-49ad-9d9f-78eb0abf3c2e', '72f81853-b586-4173-a922-fca8f75d2029', 'typical', 'Source: INAO / Rhone wine regulations. Key white variety (Condrieu).'),
('46cbe27e-8952-49ad-9d9f-78eb0abf3c2e', 'dba42af7-9439-45fa-9f48-9255c902a9a1', 'typical', 'Source: INAO / Rhone wine regulations. White variety (Hermitage, St-Joseph, Crozes).'),
('46cbe27e-8952-49ad-9d9f-78eb0abf3c2e', '0af97a1a-dd1a-4f19-9f22-681d59ad5684', 'typical', 'Source: INAO / Rhone wine regulations. White variety (Hermitage, St-Joseph, Crozes).')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Southern Rhone (9d88d130-19c1-4f95-9e9e-c880b3bd18a4)
-- Source: Wikipedia Rhone wine / Wine Folly — Grenache (dominant), Syrah, Mourvèdre,
-- Cinsaut, Carignan, plus Grenache Blanc, Roussanne, Viognier
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('9d88d130-19c1-4f95-9e9e-c880b3bd18a4', '85b5249b-c8c2-4852-bb71-3f62eebf9d2a', 'typical', 'Source: INAO / Rhone wine regulations. Dominant red variety (Chateauneuf-du-Pape, Gigondas).'),
('9d88d130-19c1-4f95-9e9e-c880b3bd18a4', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: INAO / Rhone wine regulations. Major GSM blend component.'),
('9d88d130-19c1-4f95-9e9e-c880b3bd18a4', 'e0b7b143-5ac7-4bed-937d-732a9270b67e', 'typical', 'Source: INAO / Rhone wine regulations. GSM blend component.'),
('9d88d130-19c1-4f95-9e9e-c880b3bd18a4', '91edea39-597d-4a57-b608-723697d3512f', 'typical', 'Source: INAO / Rhone wine regulations. Traditional variety.'),
('9d88d130-19c1-4f95-9e9e-c880b3bd18a4', '03ae0646-9663-479d-a255-34959a097f26', 'typical', 'Source: INAO / Rhone wine regulations. Traditional variety.'),
('9d88d130-19c1-4f95-9e9e-c880b3bd18a4', 'd6924975-4481-45d0-9a6c-b3fcbfcb9e09', 'typical', 'Source: INAO / Rhone wine regulations. Key white variety.'),
('9d88d130-19c1-4f95-9e9e-c880b3bd18a4', '0af97a1a-dd1a-4f19-9f22-681d59ad5684', 'typical', 'Source: INAO / Rhone wine regulations.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Left Bank Bordeaux (d91e9169-ff93-4b20-bbe5-917e3564352f)
-- Source: Decanter / Wine Enthusiast / INAO — Cabernet Sauvignon dominant,
-- supported by Merlot, Cabernet Franc, Petit Verdot, Malbec
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('d91e9169-ff93-4b20-bbe5-917e3564352f', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: INAO Bordeaux regulations / Decanter. Dominant force on the Left Bank.'),
('d91e9169-ff93-4b20-bbe5-917e3564352f', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: INAO Bordeaux regulations. Supporting blend role.'),
('d91e9169-ff93-4b20-bbe5-917e3564352f', '839e54a3-75b7-451a-ae05-2bc5b5759d53', 'typical', 'Source: INAO Bordeaux regulations. Blend component.'),
('d91e9169-ff93-4b20-bbe5-917e3564352f', '9c642231-dd8e-4f9d-90fb-ef00ff27ece0', 'typical', 'Source: INAO Bordeaux regulations. Blend component.'),
('d91e9169-ff93-4b20-bbe5-917e3564352f', 'bd1973df-8960-4da3-a506-fa4af28f694b', 'typical', 'Source: INAO Bordeaux regulations. Minor blend component.'),
('d91e9169-ff93-4b20-bbe5-917e3564352f', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: INAO Bordeaux regulations. White Bordeaux.'),
('d91e9169-ff93-4b20-bbe5-917e3564352f', '8d32c133-577b-4ac8-8e4c-91637382a1b3', 'typical', 'Source: INAO Bordeaux regulations. White Bordeaux (Sauternes, Graves).')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Right Bank Bordeaux (92da4410-e6db-4951-84dc-8b27068fd210)
-- Source: Decanter / Wine Enthusiast / INAO — Merlot dominant,
-- Cabernet Franc important, Cabernet Sauvignon supporting
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('92da4410-e6db-4951-84dc-8b27068fd210', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: INAO Bordeaux regulations / Decanter. Merlot reigns supreme on the Right Bank.'),
('92da4410-e6db-4951-84dc-8b27068fd210', '839e54a3-75b7-451a-ae05-2bc5b5759d53', 'typical', 'Source: INAO Bordeaux regulations. Very important, especially in St-Emilion.'),
('92da4410-e6db-4951-84dc-8b27068fd210', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: INAO Bordeaux regulations. Supporting role.'),
('92da4410-e6db-4951-84dc-8b27068fd210', 'bd1973df-8960-4da3-a506-fa4af28f694b', 'typical', 'Source: INAO Bordeaux regulations. Minor blend component.'),
('92da4410-e6db-4951-84dc-8b27068fd210', '9c642231-dd8e-4f9d-90fb-ef00ff27ece0', 'typical', 'Source: INAO Bordeaux regulations. Minor blend component.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Languedoc (a536ef6a-cbbb-4d97-9e28-aa72dc24845b)
-- Source: Creme de Languedoc / Wikipedia Languedoc-Roussillon wine —
-- Red: Grenache, Syrah, Mourvèdre, Carignan, Cinsaut
-- White: Grenache Blanc, Bourboulenc, Clairette, Roussanne, Vermentino (Rolle), Marsanne
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('a536ef6a-cbbb-4d97-9e28-aa72dc24845b', '85b5249b-c8c2-4852-bb71-3f62eebf9d2a', 'typical', 'Source: INAO / Creme de Languedoc. Principal AOP red variety.'),
('a536ef6a-cbbb-4d97-9e28-aa72dc24845b', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: INAO / Creme de Languedoc. 40,000+ ha in Languedoc-Roussillon.'),
('a536ef6a-cbbb-4d97-9e28-aa72dc24845b', 'e0b7b143-5ac7-4bed-937d-732a9270b67e', 'typical', 'Source: INAO / Creme de Languedoc. Principal AOP red variety.'),
('a536ef6a-cbbb-4d97-9e28-aa72dc24845b', '03ae0646-9663-479d-a255-34959a097f26', 'typical', 'Source: INAO / Creme de Languedoc. Traditional variety, old vines.'),
('a536ef6a-cbbb-4d97-9e28-aa72dc24845b', '91edea39-597d-4a57-b608-723697d3512f', 'typical', 'Source: INAO / Creme de Languedoc. Traditional variety.'),
('a536ef6a-cbbb-4d97-9e28-aa72dc24845b', 'd6924975-4481-45d0-9a6c-b3fcbfcb9e09', 'typical', 'Source: INAO / Creme de Languedoc. AOP white variety.'),
('a536ef6a-cbbb-4d97-9e28-aa72dc24845b', '78dd9ccf-26ce-4fd1-8ebc-977df400522a', 'typical', 'Source: INAO / Creme de Languedoc. AOP white variety (Rolle).'),
('a536ef6a-cbbb-4d97-9e28-aa72dc24845b', '0af97a1a-dd1a-4f19-9f22-681d59ad5684', 'typical', 'Source: INAO / Creme de Languedoc. AOP white variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Provence (88dd7dd9-6aea-4f46-adfa-cd0d6800e98a)
-- Source: INAO / Wine Folly — Rose-dominated: Grenache, Syrah, Mourvèdre, Cinsaut,
-- plus Vermentino (Rolle) for whites
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('88dd7dd9-6aea-4f46-adfa-cd0d6800e98a', '85b5249b-c8c2-4852-bb71-3f62eebf9d2a', 'typical', 'Source: INAO Provence AOP regulations. Key variety for rose and reds.'),
('88dd7dd9-6aea-4f46-adfa-cd0d6800e98a', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: INAO Provence AOP regulations. Key variety.'),
('88dd7dd9-6aea-4f46-adfa-cd0d6800e98a', 'e0b7b143-5ac7-4bed-937d-732a9270b67e', 'typical', 'Source: INAO Provence AOP regulations. Best varietal wines in Bandol.'),
('88dd7dd9-6aea-4f46-adfa-cd0d6800e98a', '91edea39-597d-4a57-b608-723697d3512f', 'typical', 'Source: INAO Provence AOP regulations. Traditional rose variety.'),
('88dd7dd9-6aea-4f46-adfa-cd0d6800e98a', '78dd9ccf-26ce-4fd1-8ebc-977df400522a', 'typical', 'Source: INAO Provence AOP regulations. Key white variety (Rolle).'),
('88dd7dd9-6aea-4f46-adfa-cd0d6800e98a', '03ae0646-9663-479d-a255-34959a097f26', 'typical', 'Source: INAO Provence AOP regulations. Traditional variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Chablis (b8f54f9c-f034-4bae-9b31-41cda896930c)
-- Source: INAO / Wine Folly — 100% Chardonnay (AOP requirement)
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('b8f54f9c-f034-4bae-9b31-41cda896930c', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: INAO Chablis AOP. 100% Chardonnay — sole permitted variety for Chablis AOP.')
ON CONFLICT (region_id, grape_id) DO NOTHING;


-- ============================================================================
-- ITALY L2 REGIONS
-- Sources: Italian DOC/DOCG disciplinari, Wine Folly, Wine-Searcher
-- ============================================================================

-- Langhe (04afc0f2-0cdd-4854-b73e-c995d8b49ea5)
-- Source: Piedmont DOCG/DOC regulations — Nebbiolo (Barolo, Barbaresco),
-- Barbera, Dolcetto, Chardonnay
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('04afc0f2-0cdd-4854-b73e-c995d8b49ea5', 'f3ff9596-a5cd-40a0-a0ec-66203b2682ed', 'typical', 'Source: Piedmont DOCG regulations. Barolo and Barbaresco grape.'),
('04afc0f2-0cdd-4854-b73e-c995d8b49ea5', '69367939-67e6-4990-aba0-2ff4b1456859', 'typical', 'Source: Piedmont DOC regulations. Major variety (Barbera d''Alba DOC).'),
('04afc0f2-0cdd-4854-b73e-c995d8b49ea5', 'e302d9e2-d6d1-4f80-bcf0-c58f91214dd9', 'typical', 'Source: Piedmont DOC regulations. Traditional variety (Dolcetto d''Alba DOC).'),
('04afc0f2-0cdd-4854-b73e-c995d8b49ea5', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Piedmont DOC regulations. Langhe Chardonnay DOC.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Chianti (6c63992b-6200-4b49-968d-fea5a90da421)
-- Source: Chianti DOCG disciplinare — Sangiovese min 70-80%,
-- plus Canaiolo, Colorino, Cabernet Sauvignon, Merlot
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('6c63992b-6200-4b49-968d-fea5a90da421', '7b1637f6-dda2-4172-851e-cbb787e3827e', 'typical', 'Source: Chianti DOCG disciplinare. Min 70-80% — defining variety.'),
('6c63992b-6200-4b49-968d-fea5a90da421', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Chianti DOCG disciplinare. Permitted blending variety.'),
('6c63992b-6200-4b49-968d-fea5a90da421', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: Chianti DOCG disciplinare. Permitted blending variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Montalcino (b570f3e1-2f68-41bd-9a08-a1b76d51da6d)
-- Source: Brunello di Montalcino DOCG disciplinare — 100% Sangiovese
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('b570f3e1-2f68-41bd-9a08-a1b76d51da6d', '7b1637f6-dda2-4172-851e-cbb787e3827e', 'typical', 'Source: Brunello di Montalcino DOCG disciplinare. 100% Sangiovese (Brunello clone).')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Bolgheri (b4d58f18-a21a-4763-803a-50cdfdab719d)
-- Source: Bolgheri DOC disciplinare — Cabernet Sauvignon, Merlot, Cabernet Franc,
-- Syrah, Petit Verdot. Super Tuscan territory.
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('b4d58f18-a21a-4763-803a-50cdfdab719d', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Bolgheri DOC disciplinare. Key Super Tuscan variety (Sassicaia).'),
('b4d58f18-a21a-4763-803a-50cdfdab719d', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: Bolgheri DOC disciplinare. Key variety (Masseto).'),
('b4d58f18-a21a-4763-803a-50cdfdab719d', '839e54a3-75b7-451a-ae05-2bc5b5759d53', 'typical', 'Source: Bolgheri DOC disciplinare. Bordeaux blend component.'),
('b4d58f18-a21a-4763-803a-50cdfdab719d', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: Bolgheri DOC disciplinare. Permitted variety.'),
('b4d58f18-a21a-4763-803a-50cdfdab719d', '9c642231-dd8e-4f9d-90fb-ef00ff27ece0', 'typical', 'Source: Bolgheri DOC disciplinare. Bordeaux blend component.'),
('b4d58f18-a21a-4763-803a-50cdfdab719d', '78dd9ccf-26ce-4fd1-8ebc-977df400522a', 'typical', 'Source: Bolgheri DOC disciplinare. Key white variety (Bolgheri Vermentino).')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Valpolicella (04b389de-3693-4dd5-9647-8da02d41f245)
-- Source: Valpolicella DOC / Amarone DOCG disciplinari — Corvina (45-95%),
-- Corvinone, Rondinella, Molinara, Oseleta
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('04b389de-3693-4dd5-9647-8da02d41f245', 'c832d2d3-470a-48fc-b9a6-5be8dd8c7623', 'typical', 'Source: Valpolicella DOC disciplinare. 45-95% — dominant variety (Amarone, Ripasso).'),
('04b389de-3693-4dd5-9647-8da02d41f245', '0835a859-ec16-4a9c-a951-e3d92064cb43', 'typical', 'Source: Valpolicella DOC disciplinare. Can substitute up to 50% of Corvina.'),
('04b389de-3693-4dd5-9647-8da02d41f245', 'eb39770b-7ac2-44df-85a6-50366ced3ff7', 'typical', 'Source: Valpolicella DOC disciplinare. Traditional blend component (5-30%).'),
('04b389de-3693-4dd5-9647-8da02d41f245', '3b891f6a-78d5-48fa-9399-ab8c1255ce9c', 'typical', 'Source: Valpolicella DOC disciplinare. Traditional variety, declining.'),
('04b389de-3693-4dd5-9647-8da02d41f245', '0e03ebe9-9dd6-4a64-b96d-09aa9a9064bb', 'typical', 'Source: Valpolicella DOC disciplinare. Permitted native variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Soave (bcd1cb5c-7cae-4c02-b0f6-5349e43fdd1d)
-- Source: Soave DOC disciplinare — Garganega (min 70%), plus Trebbiano di Soave, Chardonnay
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('bcd1cb5c-7cae-4c02-b0f6-5349e43fdd1d', 'c3398029-b04e-4ad9-a275-8070e3bb8810', 'typical', 'Source: Soave DOC disciplinare. Min 70% — defining variety.'),
('bcd1cb5c-7cae-4c02-b0f6-5349e43fdd1d', 'c4f35c60-61f1-49b7-ae47-03320dd8e43f', 'typical', 'Source: Soave DOC disciplinare. Trebbiano di Soave, blend component.'),
('bcd1cb5c-7cae-4c02-b0f6-5349e43fdd1d', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: Soave DOC disciplinare. Permitted blend component.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Etna (ede83af0-731e-4d5e-9356-a65a9d014fd5)
-- Source: Etna DOC disciplinare — Nerello Mascalese (min 80% reds),
-- Nerello Cappuccio, Carricante (min 60% whites)
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('ede83af0-731e-4d5e-9356-a65a9d014fd5', '01e9f1e7-8c04-4c8a-94f3-9c44ac499f63', 'typical', 'Source: Etna DOC disciplinare. Min 80% for Etna Rosso — defining red variety.'),
('ede83af0-731e-4d5e-9356-a65a9d014fd5', '241f2720-69ba-4b9e-9c95-dbd86a11b4a9', 'typical', 'Source: Etna DOC disciplinare. Nerello Cappuccio — blend component in Etna Rosso.'),
('ede83af0-731e-4d5e-9356-a65a9d014fd5', 'a9829e16-ba4a-46fb-80d5-7209989a2a3c', 'typical', 'Source: Etna DOC disciplinare. Min 60% for Etna Bianco — defining white variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;


-- ============================================================================
-- SPAIN L2 REGIONS
-- Sources: Consejo Regulador regulations, Wine Tourism Spain, Wine Folly
-- ============================================================================

-- Rioja (3e164ef6-3fdc-46a1-9f5f-19aba81147f3)
-- Source: Consejo Regulador DOCa Rioja — Tempranillo dominant,
-- Garnacha, Graciano, Mazuelo (Carignan), Viura (white)
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('3e164ef6-3fdc-46a1-9f5f-19aba81147f3', '848d868a-a4a8-4b6b-b826-2845f1aba744', 'typical', 'Source: Consejo Regulador DOCa Rioja. Dominant red variety.'),
('3e164ef6-3fdc-46a1-9f5f-19aba81147f3', '85b5249b-c8c2-4852-bb71-3f62eebf9d2a', 'typical', 'Source: Consejo Regulador DOCa Rioja. Traditional variety (Garnacha).'),
('3e164ef6-3fdc-46a1-9f5f-19aba81147f3', '1477bc18-2190-4345-a84a-85cabea56631', 'typical', 'Source: Consejo Regulador DOCa Rioja. Quality blending variety (Graciano).'),
('3e164ef6-3fdc-46a1-9f5f-19aba81147f3', '03ae0646-9663-479d-a255-34959a097f26', 'typical', 'Source: Consejo Regulador DOCa Rioja. Traditional variety (Mazuelo/Carignan).'),
('3e164ef6-3fdc-46a1-9f5f-19aba81147f3', 'd499b8db-a83c-4c7c-a2df-558ca0ba6d89', 'typical', 'Source: Consejo Regulador DOCa Rioja. Main white grape (Viura/Macabeo).')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Ribera del Duero (d8cf959d-1082-4683-b20a-7d9a8e3e53f9)
-- Source: Consejo Regulador DO Ribera del Duero — Tempranillo (Tinto Fino) dominant,
-- plus Cabernet Sauvignon, Merlot, Malbec
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('d8cf959d-1082-4683-b20a-7d9a8e3e53f9', '848d868a-a4a8-4b6b-b826-2845f1aba744', 'typical', 'Source: Consejo Regulador DO Ribera del Duero. Dominant variety (as Tinto Fino/Tinta del Pais).'),
('d8cf959d-1082-4683-b20a-7d9a8e3e53f9', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Consejo Regulador DO Ribera del Duero. Permitted variety.'),
('d8cf959d-1082-4683-b20a-7d9a8e3e53f9', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: Consejo Regulador DO Ribera del Duero. Permitted variety.'),
('d8cf959d-1082-4683-b20a-7d9a8e3e53f9', 'bd1973df-8960-4da3-a506-fa4af28f694b', 'typical', 'Source: Consejo Regulador DO Ribera del Duero. Permitted variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Priorat (c7a0637b-5e2a-4706-9f39-2d73dc3ab1f0)
-- Source: DOCa Priorat regulations — Garnacha and Carignan (old vines),
-- plus Cabernet Sauvignon, Merlot, Syrah
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('c7a0637b-5e2a-4706-9f39-2d73dc3ab1f0', '85b5249b-c8c2-4852-bb71-3f62eebf9d2a', 'typical', 'Source: DOCa Priorat regulations. Old vine Garnacha — signature variety.'),
('c7a0637b-5e2a-4706-9f39-2d73dc3ab1f0', '03ae0646-9663-479d-a255-34959a097f26', 'typical', 'Source: DOCa Priorat regulations. Old vine Carignan (Samsó) — signature variety.'),
('c7a0637b-5e2a-4706-9f39-2d73dc3ab1f0', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: DOCa Priorat regulations. Permitted variety.'),
('c7a0637b-5e2a-4706-9f39-2d73dc3ab1f0', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: DOCa Priorat regulations. Permitted variety.'),
('c7a0637b-5e2a-4706-9f39-2d73dc3ab1f0', '2af8e266-79be-4aa8-8464-06897ea20924', 'typical', 'Source: DOCa Priorat regulations. Permitted variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Rias Baixas (653c1d25-5aee-4310-a0a9-3d0519a2a747)
-- Source: DO Rias Baixas regulations — Albarino dominant (nearly 100% white production)
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('653c1d25-5aee-4310-a0a9-3d0519a2a747', 'ee766151-ddd9-4439-8af8-4ef0d7091ba2', 'typical', 'Source: DO Rias Baixas regulations. Dominant variety — virtually 100% of production.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Rueda (5ec3c751-f04d-49fa-8774-49dc03943fe7)
-- Source: DO Rueda regulations — Verdejo dominant, plus Sauvignon Blanc, Viura
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('5ec3c751-f04d-49fa-8774-49dc03943fe7', '1cf2e9ae-01e6-4fe7-a428-9a8cd6ed4346', 'typical', 'Source: DO Rueda regulations. Dominant variety — crisp, herbaceous style.'),
('5ec3c751-f04d-49fa-8774-49dc03943fe7', '46944001-da10-404c-a906-01511c6b2b7d', 'typical', 'Source: DO Rueda regulations. Permitted variety.'),
('5ec3c751-f04d-49fa-8774-49dc03943fe7', 'd499b8db-a83c-4c7c-a2df-558ca0ba6d89', 'typical', 'Source: DO Rueda regulations. Viura/Macabeo — traditional blending variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Jerez (6ba64ebc-7aea-4369-8e4c-be9417daed1b)
-- Source: DO Jerez-Xeres-Sherry regulations — Palomino Fino (backbone of Sherry),
-- Pedro Ximenez, Moscatel
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('6ba64ebc-7aea-4369-8e4c-be9417daed1b', 'a1d7f1d8-a4ab-40f2-97d3-9ce2f190a131', 'typical', 'Source: DO Jerez regulations. Backbone of Sherry — dominant variety (~95% of plantings).'),
('6ba64ebc-7aea-4369-8e4c-be9417daed1b', '13d7fa74-1506-4137-8dac-bd016c6df6db', 'typical', 'Source: DO Jerez regulations. Sweet Sherry (PX) variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;


-- ============================================================================
-- GERMANY L1 REGIONS
-- Sources: Wines of Germany (winesofgermany.co.uk) regional profiles, DWI statistics
-- ============================================================================

-- Pfalz (47d48342-a0c2-4b4e-baf5-cea04d4e2be1)
-- Source: Wines of Germany — Riesling 25.2%, Dornfelder 10.7%, Pinot Gris 9%,
-- Spatburgunder 7.2%, Muller-Thurgau 6.9%. Largest Riesling-growing region.
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('47d48342-a0c2-4b4e-baf5-cea04d4e2be1', '5ea633a0-a830-4a67-83dd-536108358c41', 'typical', 'Source: Wines of Germany / DWI 2022. 25.2% — largest Riesling-growing region in the world.'),
('47d48342-a0c2-4b4e-baf5-cea04d4e2be1', '4b1a82e9-36bf-446e-a405-572db911d8d9', 'typical', 'Source: Wines of Germany / DWI 2022. 10.7%.'),
('47d48342-a0c2-4b4e-baf5-cea04d4e2be1', '55b05ae2-de32-4951-82f4-be5703e5b9c7', 'typical', 'Source: Wines of Germany / DWI 2022. 9% (Grauburgunder).'),
('47d48342-a0c2-4b4e-baf5-cea04d4e2be1', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: Wines of Germany / DWI 2022. 7.2% (Spatburgunder). Germany''s largest red wine region.'),
('47d48342-a0c2-4b4e-baf5-cea04d4e2be1', 'bc7e8ac7-206c-4731-96d1-dbad87df3597', 'typical', 'Source: Wines of Germany / DWI 2022. 6.9%.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Nahe (65c291e8-b7f9-4abe-afe4-29aae897ff21)
-- Source: Wines of Germany — Riesling most planted, plus Burgundy varieties,
-- Muller-Thurgau, Silvaner. 4,250 ha total.
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('65c291e8-b7f9-4abe-afe4-29aae897ff21', '5ea633a0-a830-4a67-83dd-536108358c41', 'typical', 'Source: Wines of Germany Nahe profile. Most planted variety.'),
('65c291e8-b7f9-4abe-afe4-29aae897ff21', 'bc7e8ac7-206c-4731-96d1-dbad87df3597', 'typical', 'Source: Wines of Germany Nahe profile.'),
('65c291e8-b7f9-4abe-afe4-29aae897ff21', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: Wines of Germany Nahe profile. Burgundy variety (Spatburgunder).'),
('65c291e8-b7f9-4abe-afe4-29aae897ff21', '55b05ae2-de32-4951-82f4-be5703e5b9c7', 'typical', 'Source: Wines of Germany Nahe profile. Burgundy variety (Grauburgunder).'),
('65c291e8-b7f9-4abe-afe4-29aae897ff21', 'f83e7bb6-3e10-4824-a7ba-d7775afc566d', 'typical', 'Source: Wines of Germany Nahe profile. Silvaner.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Ahr (1b0043e3-c36a-4fc9-915a-91f50e8103b7)
-- Source: Wines of Germany — 79% red grapes, Spatburgunder ~65% (342 ha of 531 ha)
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('1b0043e3-c36a-4fc9-915a-91f50e8103b7', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: Wines of Germany Ahr profile. ~65% of plantings (342 ha) — highest red wine % of any German region.'),
('1b0043e3-c36a-4fc9-915a-91f50e8103b7', '5ea633a0-a830-4a67-83dd-536108358c41', 'typical', 'Source: Wines of Germany Ahr profile. Leading white variety.'),
('1b0043e3-c36a-4fc9-915a-91f50e8103b7', '6e406be9-17ce-421f-81dc-046ce92dfe58', 'typical', 'Source: Wines of Germany Ahr profile. Burgundy variety (Weissburgunder).')
ON CONFLICT (region_id, grape_id) DO NOTHING;


-- ============================================================================
-- HUNGARY REGIONS
-- Sources: Taste Hungary, Hungarian Wine Society, Wikipedia Hungarian wine,
-- hungarianwines.eu
-- ============================================================================

-- Tokaj (2a1632d9-5eb5-4341-89a0-77d1fbd20c2a)
-- Source: Taste Hungary / Wikipedia Tokaji — Furmint ~60%, Harslevelu ~30%
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('2a1632d9-5eb5-4341-89a0-77d1fbd20c2a', '77da1636-29ec-42b3-9459-52c6d300c43d', 'typical', 'Source: Taste Hungary / Tokaji regulations. ~60% of plantings — primary variety.'),
('2a1632d9-5eb5-4341-89a0-77d1fbd20c2a', '29ecc344-2430-44e5-a376-074ccf325d8e', 'typical', 'Source: Taste Hungary / Tokaji regulations. ~30% of plantings — secondary variety.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Eger (454c3d0d-f1f4-4cde-b0dc-bb89dc33da86)
-- Source: Taste Hungary / Hungarian Wine Society — Kekfrankos (Blaufrankisch) primary,
-- Kadarka, Pinot Noir, Bikaver blends. Plus Chardonnay whites.
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('454c3d0d-f1f4-4cde-b0dc-bb89dc33da86', '53d7b98c-cb10-4e82-a2da-2fe264ced8f7', 'typical', 'Source: Taste Hungary / hungarianwines.eu. Kekfrankos — base of Egri Bikaver (Bull''s Blood).'),
('454c3d0d-f1f4-4cde-b0dc-bb89dc33da86', 'a428d21a-6468-4a89-b6ae-1dfb35b412ef', 'typical', 'Source: Taste Hungary. Traditional Bikaver component.'),
('454c3d0d-f1f4-4cde-b0dc-bb89dc33da86', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: hungarianwines.eu. Permitted variety.'),
('454c3d0d-f1f4-4cde-b0dc-bb89dc33da86', '0b466398-e87f-4f5d-94db-3503651d46fe', 'typical', 'Source: hungarianwines.eu. Leading white variety (Egri Csillag blends).')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Villany (6fdd471a-7a37-4445-b645-c09c882b8682)
-- Source: Taste Hungary / hungarianwines.eu — Cabernet Sauvignon, Cabernet Franc,
-- Merlot, Portugieser, Pinot Noir (warm southern region, robust reds)
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('6fdd471a-7a37-4445-b645-c09c882b8682', '3874b50e-5cf2-40a3-bbaa-4546a7daf7d5', 'typical', 'Source: Taste Hungary / hungarianwines.eu. Major variety — robust, full-bodied reds.'),
('6fdd471a-7a37-4445-b645-c09c882b8682', '839e54a3-75b7-451a-ae05-2bc5b5759d53', 'typical', 'Source: Taste Hungary / hungarianwines.eu. Important Bordeaux variety.'),
('6fdd471a-7a37-4445-b645-c09c882b8682', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: Taste Hungary / hungarianwines.eu.'),
('6fdd471a-7a37-4445-b645-c09c882b8682', 'e257462e-d502-4270-93a7-f9fe72372460', 'typical', 'Source: Taste Hungary. Blauer Portugieser — traditional variety.'),
('6fdd471a-7a37-4445-b645-c09c882b8682', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: hungarianwines.eu. Occasionally produced.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Szekszard (b149a8a5-317b-45e1-b6db-3ef5be6ae95a)
-- Source: Taste Hungary — Kadarka, Kekfrankos, Cabernet Franc, Merlot.
-- Full-bodied reds with spice.
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('b149a8a5-317b-45e1-b6db-3ef5be6ae95a', 'a428d21a-6468-4a89-b6ae-1dfb35b412ef', 'typical', 'Source: Taste Hungary. Traditional variety — best Kadarkas from Szekszard.'),
('b149a8a5-317b-45e1-b6db-3ef5be6ae95a', '53d7b98c-cb10-4e82-a2da-2fe264ced8f7', 'typical', 'Source: Taste Hungary. Kekfrankos — major variety, top quality.'),
('b149a8a5-317b-45e1-b6db-3ef5be6ae95a', '839e54a3-75b7-451a-ae05-2bc5b5759d53', 'typical', 'Source: Taste Hungary. Cabernet Franc.'),
('b149a8a5-317b-45e1-b6db-3ef5be6ae95a', 'a82fa01d-b1d3-4c3e-bec7-fa7b6d971091', 'typical', 'Source: Taste Hungary.')
ON CONFLICT (region_id, grape_id) DO NOTHING;

-- Sopron (ca3e378e-3f51-46e2-ad31-92d155d5f394)
-- Source: Taste Hungary / hungarianwines.eu — "Capital of Kekfrankos",
-- limestone and slate hills facing Lake Neusiedl
INSERT INTO region_grapes (region_id, grape_id, association_type, notes) VALUES
('ca3e378e-3f51-46e2-ad31-92d155d5f394', '53d7b98c-cb10-4e82-a2da-2fe264ced8f7', 'typical', 'Source: Taste Hungary. "Capital of Kekfrankos" — defining variety of the region.'),
('ca3e378e-3f51-46e2-ad31-92d155d5f394', '8b5b0611-b9e3-41ab-93a9-78da68a80d7c', 'typical', 'Source: hungarianwines.eu. Zweigelt — Austrian-influenced.'),
('ca3e378e-3f51-46e2-ad31-92d155d5f394', '9f03fb29-113c-4784-8bf7-f8ebd27d1497', 'typical', 'Source: hungarianwines.eu. Cool-climate Pinot Noir.')
ON CONFLICT (region_id, grape_id) DO NOTHING;
