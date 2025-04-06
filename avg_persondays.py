import requests
import json
import statistics
import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import anthropic
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nregs_persondays_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

def get_nregs_persondays_data(date, district=None):
    """
    Fetch average persondays data from the NREGS MP dashboard API
    
    Args:
        date (str): Date in YYYY-MM-DD format
        district (str, optional): District name if specific district data is needed
    
    Returns:
        dict: API response data
    """
    base_url = "https://dashboard.nregsmp.org/api/employment_workers/avg-persondays"
    
    if district:
        url = f"{base_url}?date={date}&district={district}"
    else:
        url = f"{base_url}?date={date}"
    
    logger.info(f"Fetching persondays data from: {url}")
    response = requests.get(url)
    
    if response.status_code == 200:
        logger.info(f"Successfully fetched persondays data from: {url}")
        return response.json()
    else:
        logger.error(f"Failed to fetch persondays data: {response.status_code}")
        return None

def process_state_persondays_data(data):
    """
    Process state-level persondays data to extract top and bottom districts and state average
    
    Args:
        data (dict): State-level NREGS persondays data
    
    Returns:
        dict: Processed data with top/bottom districts and state average
    """
    if not data or 'results' not in data:
        logger.error("Invalid state persondays data format")
        return None
    
    logger.info(f"Processing state persondays data with {len(data['results'])} districts")
    
    # Format all data points to have 2 decimal places
    for district in data['results']:
        district['avg_persondays'] = round(district['avg_persondays'], 2)
        district['pd_marks'] = round(district['pd_marks'], 2)
    
    # Sort districts by avg_persondays (highest to lowest)
    sorted_districts = sorted(data['results'], key=lambda x: x['avg_persondays'], reverse=True)
    
    # Add rank to each district (for finding specific district rank later)
    district_ranks = {}
    for i, district in enumerate(sorted_districts):
        district_ranks[district['group_name']] = i + 1
    
    # Extract top 1 and bottom 1 districts
    top_district = sorted_districts[0]
    bottom_district = sorted_districts[-1]
    
    # Calculate state average
    all_avg_persondays = [district['avg_persondays'] for district in data['results']]
    state_avg_persondays = round(statistics.mean(all_avg_persondays), 2)
    
    # Calculate average marks
    avg_marks = round(statistics.mean([district['pd_marks'] for district in data['results']]), 2)
    
    logger.info(f"Top district: {top_district['group_name']} with avg_persondays {top_district['avg_persondays']}")
    logger.info(f"Bottom district: {bottom_district['group_name']} with avg_persondays {bottom_district['avg_persondays']}")
    logger.info(f"State average persondays: {state_avg_persondays}")
    
    return {
        "top_district": top_district,
        "bottom_district": bottom_district,
        "state_averages": {
            "avg_persondays": state_avg_persondays,
            "pd_marks": avg_marks
        },
        "district_ranks": district_ranks,
        "total_districts": len(sorted_districts)
    }

def process_district_persondays_data(data):
    """
    Process district-level persondays data to summarize block information
    
    Args:
        data (dict): District-level NREGS persondays data
    
    Returns:
        dict: Processed data with block information and district summary
    """
    if not data or 'results' not in data:
        logger.error("Invalid district persondays data format")
        return None
    
    logger.info(f"Processing district persondays data with {len(data['results'])} blocks")
    
    # Format all data points to have 2 decimal places
    for block in data['results']:
        block['avg_persondays'] = round(block['avg_persondays'], 2)
        block['pd_marks'] = round(block['pd_marks'], 2)
    
    # Sort blocks by avg_persondays (highest to lowest)
    sorted_blocks = sorted(data['results'], key=lambda x: x['avg_persondays'], reverse=True)
    
    # Calculate district averages
    block_avg_persondays = [block['avg_persondays'] for block in data['results']]
    avg_persondays = round(statistics.mean(block_avg_persondays), 2)
    
    avg_pd_marks = round(statistics.mean([block['pd_marks'] for block in data['results']]), 2)
    
    # Get highest and lowest performing blocks
    highest_block = sorted_blocks[0]
    lowest_block = sorted_blocks[-1]
    
    logger.info(f"Highest performing block: {highest_block['group_name']} with avg_persondays {highest_block['avg_persondays']}")
    logger.info(f"Lowest performing block: {lowest_block['group_name']} with avg_persondays {lowest_block['avg_persondays']}")
    logger.info(f"District average persondays: {avg_persondays}")
    
    return {
        "blocks": sorted_blocks,
        "district_summary": {
            "total_blocks": len(data['results']),
            "average_persondays": avg_persondays,
            "average_pd_marks": avg_pd_marks,
            "highest_performing_block": highest_block['group_name'],
            "highest_avg_persondays": highest_block['avg_persondays'],
            "highest_pd_marks": highest_block['pd_marks'],
            "lowest_performing_block": lowest_block['group_name'],
            "lowest_avg_persondays": lowest_block['avg_persondays'],
            "lowest_pd_marks": lowest_block['pd_marks']
        }
    }

def generate_persondays_analysis(state_data, district_data, target_district):
    """
    Generate analysis report using Claude 3.7 with thinking mode
    
    Args:
        state_data (dict): Processed state data
        district_data (dict): Processed district data
        target_district (str): Name of the target district
    
    Returns:
        str: Analysis report
    """
    # Create prompt template
    prompt = """
You are an AI analyst tasked with evaluating the performance of districts and blocks in Madhya Pradesh, India,
based on the National Rural Employment Guarantee Act (NREGA) data. Your goal is to provide a concise, 
professional analysis of a target district's performance in relation to the state's top and bottom performers,
as well as an assessment of the blocks within the target district.

You will be provided with the following data:

<state_data>
{state_data}
</state_data>

<district_data>
{district_data}
</district_data>

The target district for analysis is:
<target_district>{target_district}</target_district>

Analyze the provided data and generate a brief, professional report that includes:

1. A comparison of the target district's performance to the state's top and bottom performers, highlighting strengths and weaknesses.
2. An evaluation of the blocks within the target district, identifying high-performing and underperforming blocks.

Your analysis should be precise, concise, and use professional language.
Focus on the "Average Person days provided to Job card holders" metric, which is represented by the "avg_persondays" value in the data.
The scoring is calculated as follows:
- Full marks (10) if average person days per active household is >= 60 days
- Zero marks if average is less than 30 person days per active household
- Otherwise, proportionate marks are given

Present your analysis in the following format:

<analysis>
<district_performance>
[Provide a 2-3 sentence analysis of the target district's performance compared to the top and bottom districts in the state. Highlight key strengths and weaknesses.]
</district_performance>

<block_performance>
[Provide a 2-3 sentence analysis of the blocks within the target district, identifying the highest and lowest performers and any notable trends.]
</block_performance>

<recommendations>
[Offer 1-2 concise, data-driven recommendations for improving the target district's performance.]
</recommendations>
</analysis>

Ensure that your analysis is based solely on the provided data and focuses on the most significant insights that can be derived from the information given.Your response should
contain data to validate your points. give key insights of district,block and improvement potential. 
Include district ranking/score in the component, top districts, improvement potential and suggestions, blocks in the district who are performig good and bad. 
"""
    
    # Format the prompt with actual data
    formatted_prompt = prompt.format(
        state_data=json.dumps(state_data, indent=2),
        district_data=json.dumps(district_data, indent=2),
        target_district=target_district
    )
    
    # Log the prompt (optional, can be disabled for production)
    logger.debug(f"Prompt to Claude for persondays analysis:\n{formatted_prompt}")
    
    # Call Claude 3.7 API with thinking mode
    return call_claude_api(formatted_prompt)

def call_claude_api(prompt):
    """
    Call Claude 3.7 API with thinking mode enabled
    
    Args:
        prompt (str): Prompt to send to Claude
    
    Returns:
        str: Claude's response
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        error_msg = "ANTHROPIC_API_KEY environment variable not set"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Set model
    model = "claude-3-7-sonnet-20250219"
    logger.info(f"Using Claude model: {model} with thinking mode")
    
    try:
        client = anthropic.Anthropic(api_key=api_key)
        
        # Request creation with thinking mode
        response = client.messages.create(
            model=model,
            max_tokens=20000,
            thinking={
                "type": "enabled",
                "budget_tokens": 16000
            },
            messages=[{"role": "user", "content": prompt}]
        )
        
        # Log token usage
        prompt_tokens = response.usage.input_tokens
        completion_tokens = response.usage.output_tokens
        total_tokens = prompt_tokens + completion_tokens
        
        logger.info(f"Token usage - Prompt: {prompt_tokens}, Completion: {completion_tokens}, Total: {total_tokens}")
        
        # Check and log thinking output
        thinking_output = None
        if hasattr(response, 'thinking') and response.thinking:
            thinking_text = response.thinking.thinking_text
            thinking_tokens = response.thinking.tokens
            logger.info(f"Thinking mode used: {thinking_tokens} tokens")
            
            # Save thinking to file
            thinking_file = f"persondays_thinking_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(thinking_file, 'w', encoding='utf-8') as f:
                f.write(thinking_text)
            logger.info(f"Thinking output saved to {thinking_file}")
            
            thinking_output = thinking_text
        else:
            logger.info("No thinking output received")
        
        # Extract the actual response text
        response_text = ""
        for content_block in response.content:
            if hasattr(content_block, 'text'):
                response_text += content_block.text
        
        # Save full response to file
        response_file = f"persondays_claude_response_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(response_file, 'w', encoding='utf-8') as f:
            response_data = {
                "model": model,
                "response_text": response_text,
                "thinking_text": thinking_output,
                "token_usage": {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": total_tokens,
                    "thinking_tokens": thinking_tokens if hasattr(response, 'thinking') and response.thinking else 0
                }
            }
            json.dump(response_data, f, indent=2)
        
        logger.info(f"Full response data saved to {response_file}")
        
        return response_text
    
    except Exception as e:
        logger.error(f"Error calling Claude API: {str(e)}")
        raise

def main(date=None, district=None, output_format="text"):
    """
    Main function to fetch and process NREGS persondays data, then analyze it
    
    Args:
        date (str, optional): Date in YYYY-MM-DD format
        district (str, optional): District name for specific data
        output_format (str, optional): Output format ('text' or 'json')
    
    Returns:
        Union[str, dict]: Analysis result in specified format
    """
    logger.info(f"Starting NREGS persondays analysis for district: {district}, date: {date if date else 'current'}")
    
    # Use current date if none provided
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"Using current date: {date}")
    
    # Get state-level data
    logger.info("Fetching state-level persondays data")
    state_data = get_nregs_persondays_data(date)
    if not state_data:
        logger.error("Failed to get state-level persondays data")
        return None
    
    processed_state_data = process_state_persondays_data(state_data)
    if not processed_state_data:
        logger.error("Failed to process state-level persondays data")
        return None
    
    result = {
        "date": date,
        "state_data": {
            "top_district": processed_state_data["top_district"],
            "bottom_district": processed_state_data["bottom_district"],
            "state_averages": processed_state_data["state_averages"]
        }
    }
    
    # Check if district is provided
    if not district:
        error_msg = "District name is required for analysis"
        logger.error(error_msg)
        return error_msg
    
    # Get district-level data
    logger.info(f"Fetching persondays data for district: {district}")
    district_data = get_nregs_persondays_data(date, district)
    if not district_data:
        logger.error(f"Failed to get persondays data for district: {district}")
        return None
    
    processed_district_data = process_district_persondays_data(district_data)
    if not processed_district_data:
        logger.error(f"Failed to process persondays data for district: {district}")
        return None
    
    # Get district rank
    district_rank = processed_state_data["district_ranks"].get(district, None)
    total_districts = processed_state_data["total_districts"]
    
    # Find the district data in the state data for complete information
    target_district_data = None
    for d in state_data['results']:
        if d['group_name'] == district:
            target_district_data = d
            break
    
    result["district_data"] = {
        "district_name": district,
        "state_rank": district_rank,
        "total_districts": total_districts,
        "district_info": target_district_data,
        "details": processed_district_data
    }
    
    # Generate analysis using Claude
    logger.info("Generating persondays analysis using Claude 3.7")
    analysis = generate_persondays_analysis(
        result["state_data"], 
        result["district_data"], 
        district
    )
    
    # Create output based on requested format
    if output_format == "json":
        result["analysis"] = analysis
        
        # Save output to file
        filename = f"nregs_persondays_analysis_{district.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=4)
        logger.info(f"Analysis saved to {filename}")
        
        return result
    else:
        # Save output to file
        filename = f"nregs_persondays_analysis_{district.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(analysis)
        logger.info(f"Analysis saved to {filename}")
        
        return analysis

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='NREGS Persondays Data Analysis')
    parser.add_argument('--date', type=str, help='Date in YYYY-MM-DD format')
    parser.add_argument('--district', type=str, required=True, help='District name')
    parser.add_argument('--output', type=str, default="text", choices=["text", "json"],
                        help='Output format (text or json)')
    
    args = parser.parse_args()
    
    try:
        result = main(args.date, args.district, args.output)
        
        if args.output == "json":
            print(json.dumps(result, indent=4))
        else:
            print(result)
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        print(f"Error: {str(e)}")