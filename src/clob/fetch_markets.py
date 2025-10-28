"""
Fetch all markets from Polymarket and save to JSON files.

This script fetches all markets using py_clob_client and saves:
1. Full market data to markets.json
2. Market names/questions to market_names.json
"""
import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

from py_clob_client.client import ClobClient


DEFAULT_API_URL = "https://clob.polymarket.com"
DEFAULT_OUTPUT_FILENAME = "markets.json"
DEFAULT_NAMES_OUTPUT_FILENAME = "market_names.json"
DEFAULT_CURRENT_MARKETS_FILENAME = "current_markets.json"
DEFAULT_CURRENT_NAMES_FILENAME = "current_market_names.json"
MAX_PAGES = 100  # Safety limit to prevent infinite loops


def fetch_all_markets(api_url: str = DEFAULT_API_URL, max_pages: int = MAX_PAGES) -> List[Dict[str, Any]]:
	"""
	Fetch all Polymarket markets with pagination.
	
	Args:
		api_url: Polymarket CLOB API base URL
		max_pages: Maximum number of pages to fetch (safety limit)
	
	Returns:
		List of market dictionaries
	"""
	client = ClobClient(api_url)
	all_markets = []
	cursor = "MA=="
	page_count = 0
	
	print("Fetching markets...", file=sys.stderr)
	
	while page_count < max_pages:
		try:
			response = client.get_markets(next_cursor=cursor)
			
			# Extract data from response
			if isinstance(response, dict) and "data" in response:
				markets = response["data"]
				all_markets.extend(markets)
				
				# Get next cursor
				next_cursor = response.get("next_cursor")
				
				page_count += 1
				print(f"  Page {page_count}: fetched {len(markets)} markets (total: {len(all_markets)})", file=sys.stderr)
				
				# Stop if no more pages
				if not next_cursor or next_cursor == cursor:
					break
					
				cursor = next_cursor
			else:
				# Unexpected response format
				break
				
		except Exception as e:
			print(f"Error on page {page_count + 1}: {e}", file=sys.stderr)
			break
	
	print(f"✓ Fetched {len(all_markets)} total markets", file=sys.stderr)
	return all_markets


def filter_current_markets(markets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
	"""
	Filter for only current/active markets that are still open for trading.
	
	A market is considered "current" if:
	- closed == False (market hasn't been resolved yet)
	- archived == False (market is still visible/tradeable)
	
	Args:
		markets: List of market dictionaries
	
	Returns:
		List of current/open market dictionaries
	"""
	current = []
	
	for market in markets:
		if not isinstance(market, dict):
			continue
		
		# A market is "current" if it's not closed and not archived
		is_closed = market.get("closed", True)  # Default to closed if missing
		is_archived = market.get("archived", True)  # Default to archived if missing
		
		# Market is current if it's still open (not closed and not archived)
		if not is_closed and not is_archived:
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
	parser.add_argument(
		"--current",
		action="store_true",
		help="Filter and save only current/open markets (saves to current_markets.json and current_market_names.json)"
	)
	
	args = parser.parse_args()
	
	# Set output paths
	script_dir = Path(__file__).parent
	markets_path = Path(args.out) if args.out else script_dir / DEFAULT_OUTPUT_FILENAME
	names_path = Path(args.names_out) if args.names_out else script_dir / DEFAULT_NAMES_OUTPUT_FILENAME
	
	try:
		# Fetch all markets
		all_markets = fetch_all_markets(api_url=args.api_url, max_pages=args.max_pages)
		
		if not all_markets:
			print("⚠ No markets fetched", file=sys.stderr)
			return 1
		
		# If --current flag, only save current markets
		if args.current:
			current_markets = filter_current_markets(all_markets)
			print(f"✓ Filtered to {len(current_markets)} current markets (from {len(all_markets)} total)")
			
			if not current_markets:
				print("⚠ No current markets found", file=sys.stderr)
				return 1
			
			# Use separate filenames for current markets
			script_dir = Path(__file__).parent
			current_markets_path = script_dir / DEFAULT_CURRENT_MARKETS_FILENAME
			current_names_path = script_dir / DEFAULT_CURRENT_NAMES_FILENAME
			
			# Save current markets
			save_json(current_markets, current_markets_path, indent=args.indent)
			print(f"✓ Saved {len(current_markets)} current markets to {current_markets_path}")
			
			# Extract and save current market names
			names = extract_market_names(current_markets)
			save_json(names, current_names_path, indent=args.indent)
			print(f"✓ Saved {len(names)} current market names to {current_names_path}")
		else:
			# Save all markets
			save_json(all_markets, markets_path, indent=args.indent)
			print(f"✓ Saved {len(all_markets)} markets to {markets_path}")
			
			# Extract and save all market names
			names = extract_market_names(all_markets)
			save_json(names, names_path, indent=args.indent)
			print(f"✓ Saved {len(names)} market names to {names_path}")
		
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
