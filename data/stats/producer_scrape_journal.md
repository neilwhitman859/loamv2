# Producer Website Scrape Journal

Append-only log of data improvement observations from scraping top producer websites.
These are first-party authoritative sources — discrepancies signal DB quality issues worth fixing.

## Categories of Observations

- **MISSING_PRODUCER**: Producer not in our DB at all (needs creation)
- **WRONG_DATA**: Our DB has incorrect data vs. first-party source
- **MISSING_WINE**: Wine exists on producer site but not in our DB
- **MISSING_GRAPE**: Grape link missing or wrong percentage
- **MISSING_APPELLATION**: Wine has no appellation but producer site confirms one
- **MISSING_COUNTRY**: Producer has NULL country_id
- **MISSING_METADATA**: Producer missing year_established, winemaker, description, etc.
- **DUPLICATE**: Multiple DB entries for what should be one producer
- **NAMING**: Producer or wine name doesn't match first-party usage

---

## Observations

### 2026-04-07 — Initial Batch (100 top producers)

**From pilot testing (Ridge, Opus One, Caymus):**

1. **MISSING_COUNTRY**: Ridge Vineyards has NULL country_id in our DB despite being a famous US producer
2. **MISSING_METADATA**: Ridge has no year_established (1962), no description, no winemaker in our DB
3. **MISSING_METADATA**: Opus One has no year_established (1979), no winemaker (Michael Silacci)
4. **MISSING_METADATA**: Caymus has no year_established (1972), no winemaker (Charlie Wagner)
5. **DUPLICATE**: Duckhorn has 4 entries (Duckhorn Vineyards, Duckhorn Wine Company, Duckhorn, Duckhorn Direct)
6. **DUPLICATE**: Robert Mondavi has 4 entries
7. **DUPLICATE**: Conterno has 5+ entries (Giacomo, Poderi Aldo, Conterno, Conterno Fantino, Paolo)
8. **DUPLICATE**: Leflaive has 3+ entries
9. **MISSING_PRODUCER**: Several iconic producers absent entirely: Screaming Eagle, Harlan Estate, Opus One (now created), Spottswoode, Dominus, Quilceda Creek, Leonetti, Haut-Brion, DRC
10. **MISSING_COUNTRY**: Multiple major producers have NULL country_id: Penfolds, Antinori, Krug, Robert Mondavi Winery, Stag's Leap Wine Cellars, Cheval Blanc

**General patterns:**
- Producer websites are the best source for: winemaker identity, founding year, farming practices, grape percentages
- Detail pages (individual wine tech sheets) are richest for: ABV, pH, TA, RS, oak details, production numbers
- Many sites are JS-heavy single-page apps (harder to scrape with requests)
- French chateau sites often require age verification gate (JavaScript)
- **NAMING**: Château Lafite Rothschild -- DB has 'Chateau Lafite Rothschild', producer site uses 'Château Lafite Rothschild'
- **NAMING**: Château Latour -- DB has 'Chateau Latour', producer site uses 'Château Latour'
- **NAMING**: Château Mouton Rothschild -- DB has 'Chateau Mouton Rothschild', producer site uses 'Château Mouton Rothschild'
- **NAMING**: Château Haut-Brion -- DB has 'Chateau Haut-Brion', producer site uses 'Château Haut-Brion'
- **NAMING**: Château Cheval Blanc -- DB has 'Chateau Cheval Blanc', producer site uses 'Château Cheval Blanc'
- **NAMING**: Château Palmer -- DB has 'Chateau Palmer', producer site uses 'Château Palmer'
- **NAMING**: Château Lynch-Bages -- DB has 'Lynch Bages', producer site uses 'Château Lynch-Bages'
- **NAMING**: Château Ducru-Beaucaillou -- DB has 'Chateau Ducru-Beaucaillou', producer site uses 'Château Ducru-Beaucaillou'
- **NAMING**: Maison Joseph Drouhin -- DB has 'Joseph Drouhin', producer site uses 'Maison Joseph Drouhin'
- **NAMING**: Domaine William Fèvre -- DB has 'Domaine William Fevre', producer site uses 'Domaine William Fèvre'
- **NAMING**: Dom Pérignon -- DB has 'Dom Perignon', producer site uses 'Dom Pérignon'
- **NAMING**: Château de Beaucastel -- DB has 'Chateau de Beaucastel', producer site uses 'Château de Beaucastel'
- **NAMING**: López de Heredia -- DB has 'Lopez de Heredia', producer site uses 'López de Heredia'
- **NAMING**: Joh. Jos. Prüm -- DB has 'Joh Jos Prum', producer site uses 'Joh. Jos. Prüm'

**From full batch (77 completed, 23 failed):**

11. **MISSING_METADATA**: Many top producers lack year_established. Scraper filled: Krug (1843), Chapoutier (1808), Penfolds (1844), Torbreck (1994), Dr. Loosen (1800), Mouton Rothschild (1853), Cheval Blanc (1832)
12. **MISSING_METADATA**: 28 winemakers discovered and linked — most top producers had no winemaker in our DB
13. **MISSING_WINE**: Many flagship wines missing. E.g., Guigal's La Mouline/La Turque/La Landonne not in DB, Krug Clos du Mesnil/Clos d'Ambonnay, Ridge Monte Bello, Penfolds Grange
14. **MISSING_APPELLATION**: Wines from known appellations missing appellation_id. E.g., Guigal wines in Côte-Rôtie/Condrieu/Hermitage, CVNE in Rioja, Torbreck in Barossa Valley
15. **MISSING_GRAPE**: Many wines lack grape links. Scraper added 537+ grape links across 77 producers
16. **ACCESSIBILITY**: 23% of top-100 producer sites are inaccessible via simple HTTP (JS-only or bot protection). Notably: all First Growth Bordeaux except Latour/Mouton/Haut-Brion, Gaja, Vega Sicilia, Screaming Eagle. These represent the most prestigious producers in the world.
17. **MISSING_METADATA**: Farming certifications discovered: Torbreck (sustainable), Dr. Loosen (sustainable), Louis Roederer (organic), Chapoutier (biodynamic), Leflaive (biodynamic)
18. **DEPTH_GAP**: Detail pages (tech sheets) are the richest source — 252 winemaker notes, 135 oak durations, 80 ABVs, 36 pH values from 311 new vintages. Most of our existing vintages have none of this.
19. **NAMING**: Opus One listed vintages as separate wines (e.g., "Opus One 2022") — vintage-as-name collapsing needed for many sites
20. **NAMING**: Silver Oak lists "2021 Alexander Valley Cabernet Sauvignon" and "2021 Napa Valley Cabernet Sauvignon 6L" — bottle format in wine name

**Systemic patterns discovered:**
- French châteaux are the hardest to scrape (JS-heavy, age gates, minimal text in HTML)
- US producers are the most accessible and data-rich (tech sheets, production numbers, detailed notes)
- Italian producers often return minimal text (Gaja, Tenuta San Guido completely inaccessible)
- German producers (Dr. Loosen especially) have excellent structured data
- Champagne houses have the deepest vintage data (Krug: 56 vintages across wines)
- Budget-efficient: entire 100-producer batch cost ~$1.65 in Haiku API calls
