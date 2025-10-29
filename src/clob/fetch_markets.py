"""
Fetch all markets from Polymarket and save to JSON files.

This script fetches all markets using py_clob_client and saves:
1. Full market data to markets.json
2. Market names/questions to market_names.json
"""
import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from py_clob_client.client import ClobClient


DEFAULT_API_URL = "https://clob.polymarket.com"
DEFAULT_GAMMA_API_URL = "https://gamma-api.polymarket.com"
DEFAULT_CHAIN_ID = 137
DEFAULT_OUTPUT_FILENAME = "markets.json"
DEFAULT_NAMES_OUTPUT_FILENAME = "market_names.json"
DEFAULT_CURRENT_MARKETS_FILENAME = "current_markets.json"
DEFAULT_CURRENT_NAMES_FILENAME = "current_market_names.json"
MAX_PAGES = 100  # Safety limit to prevent infinite loops


def fetch_markets_with_filter(
	next_cursor: Optional[str] = None,
	mode: Optional[str] = None,
	limit: int = 500,
	offset: Optional[int] = None
) -> Dict[str, Any]:
	"""
	Fetch markets from Gamma API with optional filtering parameters.
	
	The Gamma API supports server-side filtering, unlike the CLOB API.
	
	Args:
		next_cursor: Pagination cursor (None for first page)
		mode: Filter mode - "open", "active", "closed", or "all" (None = all)
		limit: Number of markets per page (default 500)
		offset: Offset for fallback pagination when cursor not available
	
	Returns:
		Response dictionary with 'data' and 'next_cursor' keys
	
	Modes:
		- "open": active=true AND closed=false (markets currently trading)
		- "active": active=true (all active markets, may include closed)
		- "closed": closed=true (resolved markets)
		- "all": no filters (everything)
	"""
	params = {"limit": str(limit)}
	
	# Apply mode-based filtering
	if mode == "open":
		params["closed"] = "false"
		params["active"] = "true"
	elif mode == "active":
		params["active"] = "true"
	elif mode == "closed":
		params["closed"] = "true"
	# mode == "all" or None => no filters
	
	if next_cursor:
		params["cursor"] = next_cursor
	
	if offset is not None:
		params["offset"] = str(offset)
	
	endpoint = f"{DEFAULT_GAMMA_API_URL}/markets"
	response = requests.get(endpoint, params=params, timeout=30)
	response.raise_for_status()
	
	data = response.json()
	
	# Gamma API returns data differently depending on structure
	if isinstance(data, dict):
		return {
			"data": data.get("data", []),
			"next_cursor": data.get("next_cursor"),
			"count": len(data.get("data", []))
		}
	elif isinstance(data, list):
		return {
			"data": data,
			"next_cursor": None,
			"count": len(data)
		}
	else:
		return {
			"data": [],
			"next_cursor": None,
			"count": 0
		}


def extract_market_data(market: Dict[str, Any]) -> Dict[str, Any]:
	"""
	Extract comprehensive market data from API response.
	
	Args:
		market: Raw market dictionary from API
	
	Returns:
		Cleaned market dictionary with all relevant fields
	"""
	return {
		"question": market.get("question"),
		"condition_id": market.get("condition_id"),
		"question_id": market.get("question_id"),
		"market_slug": market.get("market_slug"),
		"end_date_iso": market.get("end_date_iso"),
		"game_start_time": market.get("game_start_time"),
		"category": market.get("category"),
		"active": market.get("active", False),
		"closed": market.get("closed", True),
		"archived": market.get("archived", True),
		"accepting_orders": market.get("accepting_orders", False),
		"minimum_order_size": market.get("minimum_order_size"),
		"minimum_tick_size": market.get("minimum_tick_size"),
		"seconds_delay": market.get("seconds_delay"),
		"fpmm": market.get("fpmm"),
		"icon": market.get("icon"),
		"description": market.get("description"),
		"tokens": market.get("tokens", []),
		"rewards": market.get("rewards"),
	}


def fetch_all_markets(
	api_url: str = DEFAULT_API_URL, 
	chain_id: int = DEFAULT_CHAIN_ID,
	max_pages: int = MAX_PAGES,
	mode: Optional[str] = None,
	limit: int = 500
) -> List[Dict[str, Any]]:
	"""
	Fetch all Polymarket markets with pagination.
	
	Args:
		api_url: Polymarket CLOB API base URL (used when mode is None)
		chain_id: Blockchain chain ID (137 for Polygon)
		max_pages: Maximum number of pages to fetch (safety limit)
		mode: Filter mode - "open", "active", "closed", or "all" (None = use CLOB API)
		limit: Number of markets per page (default 500)
	
	Returns:
		List of market dictionaries
	
	Modes:
		- "open": active=true AND closed=false (markets currently trading)
		- "active": active=true (all active markets)
		- "closed": closed=true (resolved markets)
		- "all": no filters via Gamma API (everything)
		- None: use CLOB API (full details, no filtering)
	
	Note:
		When mode is specified, the Gamma API is used which supports filtering.
		When mode is None, the CLOB API is used which returns full market details.
	"""
	all_markets = []
	cursor: Optional[str] = None if mode else "MA=="
	offset: Optional[int] = None
	page_count = 0
	total_fetched = 0
	
	mode_text = f" (mode={mode})" if mode else ""
	print(f"Fetching markets{mode_text}...", file=sys.stderr)
	
	while page_count < max_pages:
		try:
			# Use Gamma API with filtering when mode is specified
			if mode:
				response = fetch_markets_with_filter(
					next_cursor=cursor,
					mode=mode,
					limit=limit,
					offset=offset
				)
			else:
				# Use CLOB client for unfiltered requests
				client = ClobClient(api_url, chain_id=chain_id)
				response = client.get_markets(next_cursor=cursor)
				# Normalize response format
				if isinstance(response, dict) and "data" in response:
					response["count"] = len(response["data"])
			
			# Extract data from response
			if isinstance(response, dict) and "data" in response:
				markets_raw = response["data"]
				count = len(markets_raw)
				total_fetched += count
				
				# Process markets
				for m in markets_raw:
					# Extract comprehensive market data
					market = extract_market_data(m)
					all_markets.append(market)
				
				# Get next cursor
				next_cursor = response.get("next_cursor")
				
				page_count += 1
				print(f"  Batch {page_count}: +{count} fetched (total: {len(all_markets)}), next_cursor={next_cursor}, offset={offset}", file=sys.stderr)
				
				# Stop if no more pages
				if not next_cursor or (cursor and next_cursor == cursor):
					# Check if we should try offset pagination
					if mode and count == limit:
						# Fallback to offset paging when full batch but no cursor
						cursor = None
						offset = limit if offset is None else offset + limit
					else:
						break
				else:
					# Use cursor for next page
					cursor = next_cursor
					offset = None
				
				# Stop if we got fewer results than expected (likely last page)
				if count < limit:
					break
				
				# Add respectful delay between requests
				time.sleep(0.25)
			else:
				# Unexpected response format
				break
				
		except Exception as e:
			print(f"Error on batch {page_count + 1}: {e}", file=sys.stderr)
			break
	
	print(f"✓ Fetched {len(all_markets)} total markets in {page_count} batches", file=sys.stderr)
	return all_markets


def filter_current_markets(markets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
	"""
	Filter for only current/active markets that are still open for trading.
	
	A market is considered "current" if:
	- active == True (market is actively trading)
	- closed == False (market hasn't been resolved yet)
	
	Args:
		markets: List of market dictionaries
	
	Returns:
		List of current/open market dictionaries
	"""
	current = []
	
	for market in markets:
		if not isinstance(market, dict):
			continue
		
		# A market is "current" if it's active and not closed
		is_active = market.get("active", False)  # Default to inactive if missing
		is_closed = market.get("closed", True)  # Default to closed if missing
		
		# Market is current if it's active and not closed
		if is_active and not is_closed:
			current.append(market)
	
	return current


def extract_market_names(markets: List[Dict[str, Any]]) -> List[str]:
	"""
	Extract market names/questions from market data.
	
	Args:
		markets: List of market dictionaries
	
	Returns:
		List of market names
	"""
	names = []
	
	for market in markets:
		if not isinstance(market, dict):
			continue
		
		# Try different name fields in order of preference
		name = None
		for field in ["question", "title", "name", "description", "market_slug"]:
			value = market.get(field)
			if isinstance(value, str) and value.strip():
				name = value.strip()
				break
		
		if name:
			names.append(name)
	
	return names


def save_json(data: Any, filepath: Path, indent: int = 2) -> None:
	"""
	Save data to JSON file.
	
	Args:
		data: Data to save
		filepath: Output file path
		indent: JSON indentation
	"""
	filepath.parent.mkdir(parents=True, exist_ok=True)
	with filepath.open("w", encoding="utf-8") as f:
		json.dump(data, f, ensure_ascii=False, indent=indent)


def create_market_metadata(markets: List[Dict[str, Any]], only_open: bool = False, total_original: int = None) -> Dict[str, Any]:
	"""
	Create metadata wrapper for market data.
	
	Args:
		markets: List of market dictionaries
		only_open: Whether this is filtered to only open markets
		total_original: Total number of markets before filtering (if filtered)
	
	Returns:
		Dictionary with metadata and markets
	"""
	metadata = {
		"timestamp": time.time(),
		"only_open_markets": only_open,
		"total_markets": len(markets),
	}
	
	if only_open and total_original is not None:
		metadata["total_original_markets"] = total_original
	
	# Extract all asset IDs
	asset_ids = []
	for market in markets:
		for token in market.get("tokens", []):
			token_id = token.get("token_id")
			if token_id:
				asset_ids.append(token_id)
	
	metadata["total_asset_ids"] = len(asset_ids)
	metadata["markets"] = markets
	
	return metadata


def main() -> int:
	"""Main entry point."""
	parser = argparse.ArgumentParser(
		description="Fetch all Polymarket markets and save to JSON files."
	)
	parser.add_argument(
		"--api-url",
		default=DEFAULT_API_URL,
		help=f"Polymarket CLOB API URL (default: {DEFAULT_API_URL})"
	)
	parser.add_argument(
		"--out",
		type=str,
		help=f"Output path for full markets (default: src/clob/{DEFAULT_OUTPUT_FILENAME})"
	)
	parser.add_argument(
		"--names-out",
		type=str,
		help=f"Output path for market names (default: src/clob/{DEFAULT_NAMES_OUTPUT_FILENAME})"
	)
	parser.add_argument(
		"--indent",
		type=int,
		default=2,
		help="JSON indentation spaces (default: 2)"
	)
	parser.add_argument(
		"--max-pages",
		type=int,
		default=MAX_PAGES,
		help=f"Maximum pages to fetch (default: {MAX_PAGES})"
	)
	# Mutually exclusive group for market filtering modes
	mode_group = parser.add_mutually_exclusive_group()
	mode_group.add_argument(
		"--current",
		action="store_true",
		help="Fetch only current/open markets (active=true AND closed=false)"
	)
	mode_group.add_argument(
		"--active",
		action="store_true",
		help="Fetch only active markets (active=true, may include closed)"
	)
	mode_group.add_argument(
		"--all",
		action="store_true",
		help="Fetch all markets via Gamma API (no filters)"
	)
	mode_group.add_argument(
		"--closed",
		action="store_true",
		help="Fetch only closed/resolved markets (closed=true)"
	)
	
	args = parser.parse_args()
	
	# Set output paths
	script_dir = Path(__file__).parent
	markets_path = Path(args.out) if args.out else script_dir / DEFAULT_OUTPUT_FILENAME
	names_path = Path(args.names_out) if args.names_out else script_dir / DEFAULT_NAMES_OUTPUT_FILENAME
	
	try:
		# Determine which mode to use based on flags
		mode: Optional[str] = None
		mode_name = "all (CLOB API)"
		use_special_filenames = False
		
		if args.current:
			mode = "open"
			mode_name = "current/open"
			use_special_filenames = True
		elif args.active:
			mode = "active"
			mode_name = "active"
		elif args.closed:
			mode = "closed"
			mode_name = "closed"
		elif args.all:
			mode = "all"
			mode_name = "all (Gamma API)"
		# else: mode stays None (use CLOB API with no filtering)
		
		# Fetch all markets
		all_markets = fetch_all_markets(
			api_url=args.api_url, 
			chain_id=DEFAULT_CHAIN_ID,
			max_pages=args.max_pages,
			mode=mode,
			limit=500
		)
		
		if not all_markets:
			print(f"⚠ No {mode_name} markets fetched", file=sys.stderr)
			return 1
		
		# Determine output paths and create metadata
		if use_special_filenames:
			# Use separate filenames for current markets
			script_dir = Path(__file__).parent
			output_markets_path = script_dir / DEFAULT_CURRENT_MARKETS_FILENAME
			output_names_path = script_dir / DEFAULT_CURRENT_NAMES_FILENAME
		else:
			output_markets_path = markets_path
			output_names_path = names_path
		
		# Create metadata wrapper
		market_data = create_market_metadata(all_markets, only_open=(mode == "open"))
		market_data["mode"] = mode if mode else "clob_api"
		
		# Save markets with metadata
		save_json(market_data, output_markets_path, indent=args.indent)
		print(f"✓ Saved {len(all_markets)} {mode_name} markets to {output_markets_path}")
		print(f"  - Total asset IDs: {market_data['total_asset_ids']}")
		
		# Extract and save market names
		names = extract_market_names(all_markets)
		save_json(names, output_names_path, indent=args.indent)
		print(f"✓ Saved {len(names)} {mode_name} market names to {output_names_path}")
		
		return 0
		
	except KeyboardInterrupt:
		print("\n⚠ Interrupted by user", file=sys.stderr)
		return 130
	except Exception as e:
		print(f"✗ Error: {e}", file=sys.stderr)
		import traceback
		traceback.print_exc()
		return 1


if __name__ == "__main__":
	sys.exit(main())
