# Polymarket Markets Fetcher

Fetch markets from Polymarket with flexible filtering options.

## Usage

```bash
# Fetch open markets (active AND not closed)
python fetch_markets.py --current

# Fetch all active markets (may include closed)
python fetch_markets.py --active

# Fetch all markets via Gamma API (no filters)
python fetch_markets.py --all

# Fetch only closed/resolved markets
python fetch_markets.py --closed

# Fetch using CLOB API (full details, no filtering)
python fetch_markets.py
```

## Filtering Modes

| Mode        | API Used  | Filter                         | Markets Returned       | Details Level             |
| ----------- | --------- | ------------------------------ | ---------------------- | ------------------------- |
| `--current` | Gamma API | `active=true AND closed=false` | ~1,500 open markets    | Basic (some nulls)        |
| `--active`  | Gamma API | `active=true`                  | ~2,500+ active markets | Basic (some nulls)        |
| `--closed`  | Gamma API | `closed=true`                  | Resolved markets       | Basic (some nulls)        |
| `--all`     | Gamma API | None                           | All markets            | Basic (some nulls)        |
| (default)   | CLOB API  | None                           | All markets            | **Full details + tokens** |

## API Comparison

### Gamma API (`https://gamma-api.polymarket.com`)

- ✅ **Supports filtering** (`active`, `closed` parameters)
- ✅ Fast queries with server-side filtering
- ⚠️ Returns basic data (some fields null, tokens often empty)
- 📦 **500 markets per batch**

### CLOB API (`https://clob.polymarket.com`)

- ✅ **Full market details** (all fields populated)
- ✅ **Token data included** (condition_id, token_ids, etc.)
- ⚠️ No filtering support (returns all markets)
- 📦 **1000 markets per batch**

## Output Files

### Default Mode

- `markets.json` - Full market data with metadata
- `market_names.json` - Array of market questions

### Current Mode (`--current`)

- `current_markets.json` - Open markets with metadata
- `current_market_names.json` - Array of open market questions

## Output Format

```json
{
  "timestamp": 1234567890.123,
  "mode": "open",
  "only_open_markets": true,
  "total_markets": 1500,
  "total_asset_ids": 0,
  "markets": [
    {
      "question": "Will X happen?",
      "condition_id": "0x...",
      "active": true,
      "closed": false,
      "tokens": [...],
      ...
    }
  ]
}
```

## Advanced Options

```bash
# Limit number of pages (batches) fetched
python fetch_markets.py --active --max-pages 10

# Custom output paths
python fetch_markets.py --out custom_markets.json --names-out custom_names.json

# Custom JSON indentation
python fetch_markets.py --active --indent 4

# Different API endpoint
python fetch_markets.py --api-url https://clob.polymarket.com
```

## Examples

### Get truly open markets for trading

```bash
python fetch_markets.py --current
# → ~1,500 markets (active=true, closed=false)
# → Saves to current_markets.json
```

### Get all active markets (including resolved)

```bash
python fetch_markets.py --active
# → ~2,500+ markets (active=true, may be closed)
# → Saves to markets.json
```

### Get full details for all markets

```bash
python fetch_markets.py --max-pages 50
# → Uses CLOB API with complete token/condition data
# → Saves to markets.json with all fields populated
```

## Field Mapping

The script extracts comprehensive market data:

- **Basic Info**: question, description, category, icon/image
- **Identifiers**: condition_id, question_id, market_slug
- **Status**: active, closed, archived, accepting_orders
- **Timing**: end_date_iso, game_start_time
- **Trading**: minimum_order_size, minimum_tick_size, seconds_delay
- **Tokens**: tokens array with token_id and outcome
- **Rewards**: rewards object with min_size, max_spread, etc.
- **FPMM**: fpmm address for liquidity pool

## Pagination Strategy

The script uses intelligent pagination:

1. **Cursor-based** when API provides `next_cursor`
2. **Offset-based fallback** when cursor unavailable but full batches returned
3. **Auto-stop** when fewer results than limit (last page)
4. **Rate limiting**: 0.25s delay between requests
