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
        logging.FileHandler("nregs_category_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

def get_category_employment_data(date, district=None):
    """
    Fetch category employment data from the NREGS MP dashboard API
    
    Args:
        date (str): Date in YYYY-MM-DD format
        district (str, optional): District name if specific district data is needed
    
    Returns:
        dict: API response data
    """
    base_url = "https://dashboard.nregsmp.org/api/employment_workers/category-employment"
    
    if district:
        url = f"{base_url}?date={date}&district={district}"
    else:
        url = f"{base_url}?date={date}"
    
    logger.info(f"Fetching category employment data from: {url}")
    response = requests.get(url)
    
    if response.status_code == 200:
        logger.info(f"Successfully fetched category employment data from: {url}")
        return response.json()
    else:
        logger.error(f"Failed to fetch category employment data: {response.status_code}")
        return None

def get_disabled_employment_data(date, district=None):
    """
    Fetch disabled workers employment data from the NREGS MP dashboard API
    
    Args:
        date (str): Date in YYYY-MM-DD format
        district (str, optional): District name if specific district data is needed
    
    Returns:
        dict: API response data
    """
    base_url = "https://dashboard.nregsmp.org/api/employment_workers/disabled"
    
    if district:
        url = f"{base_url}?date={date}&district={district}"
    else:
        url = f"{base_url}?date={date}"
    
    logger.info(f"Fetching disabled employment data from: {url}")
    response = requests.get(url)
    
    if response.status_code == 200:
        logger.info(f"Successfully fetched disabled employment data from: {url}")
        return response.json()
    else:
        logger.error(f"Failed to fetch disabled employment data: {response.status_code}")
        return None

def merge_category_and_disabled_data(category_data, disabled_data):
    """
    Merge data from category employment and disabled workers APIs
    
    Args:
        category_data (dict): Category employment data
        disabled_data (dict): Disabled workers data
    
    Returns:
        dict: Merged data
    """
    if not category_data or not disabled_data or 'results' not in category_data or 'results' not in disabled_data:
        logger.error("Invalid data format for merging")
        return None
    
    # Create a mapping of disabled data by district name
    disabled_map = {item['group_name']: item for item in disabled_data['results']}
    
    # Merge the data
    merged_results = []
    for category_item in category_data['results']:
        district_name = category_item['group_name']
        merged_item = category_item.copy()
        
        # Add disabled data if available
        if district_name in disabled_map:
            disabled_item = disabled_map[district_name]
            merged_item.update({
                'persondays_generated_disabled': disabled_item['persondays_generated'],
                'employment_availed_total_persondays': disabled_item['employment_availed_total_persondays'],
                'disabled_ratio': disabled_item['disabled_ratio'],
                'disabled_marks': disabled_item['disabled_marks']
            })
        else:
            # Add placeholder values if disabled data not available
            merged_item.update({
                'persondays_generated_disabled': 0,
                'employment_availed_total_persondays': 0,
                'disabled_ratio': 0,
                'disabled_marks': 0
            })
        
        # Calculate total marks including disabled marks
        if 'total_marks' in merged_item and 'disabled_marks' in merged_item:
            merged_item['overall_total_marks'] = merged_item['total_marks'] + merged_item['disabled_marks']
        
        merged_results.append(merged_item)
    
    # Sort merged results by total marks
    merged_results = sorted(merged_results, key=lambda x: x.get('overall_total_marks', 0), reverse=True)
    
    return {
        'level': category_data.get('level'),
        'date': category_data.get('date'),
        'results': merged_results
    }

def process_state_category_data(merged_data):
    """
    Process state-level category employment data to extract top/bottom districts and state averages
    
    Args:
        merged_data (dict): Merged category and disabled employment data
    
    Returns:
        dict: Processed data with top/bottom districts and state averages
    """
    if not merged_data or 'results' not in merged_data:
        logger.error("Invalid state category data format")
        return None
    
    logger.info(f"Processing state category data with {len(merged_data['results'])} districts")
    
    # Format all data points to have 2 decimal places
    for district in merged_data['results']:
        for key, value in district.items():
            if isinstance(value, float):
                district[key] = round(value, 2)
        
        # Calculate 100 days employment percentage
        if 'hh_issued_jobcards_total' in district and district['hh_issued_jobcards_total'] > 0:
            district['hundred_days_percentage'] = round(district['families_completed_100_days_total'] / district['hh_issued_jobcards_total'] * 100, 2)
        else:
            district['hundred_days_percentage'] = 0
            
        # Calculate ST employment percentage
        if 'hh_issued_jobcards_sts' in district and district['hh_issued_jobcards_sts'] > 0:
            district['st_employment_percentage'] = round(district['no_of_hh_provided_employment_sts'] / district['hh_issued_jobcards_sts'] * 100, 2)
        else:
            district['st_employment_percentage'] = 0
            
        # Calculate SC employment percentage
        if 'hh_issued_jobcards_scs' in district and district['hh_issued_jobcards_scs'] > 0:
            district['sc_employment_percentage'] = round(district['no_of_hh_provided_employment_scs'] / district['hh_issued_jobcards_scs'] * 100, 2)
        else:
            district['sc_employment_percentage'] = 0
            
        # Calculate women person days percentage
        if 'active_workers_women' in district and district['active_workers_women'] > 0:
            district['women_pd_percentage'] = round(district['no_of_persondays_generated_women'] / district['active_workers_women'] * 100, 2)
        else:
            district['women_pd_percentage'] = 0
    
    # Sort districts by overall total marks (highest to lowest)
    sorted_districts = merged_data['results']
    
    # Add rank to each district
    district_ranks = {}
    for i, district in enumerate(sorted_districts):
        district_ranks[district['group_name']] = i + 1
    
    # Extract top 1 and bottom 1 districts
    top_district = sorted_districts[0]
    bottom_district = sorted_districts[-1]
    
    # Calculate state averages for key metrics
    avg_hundred_days_percentage = round(statistics.mean([d.get('hundred_days_percentage', 0) for d in merged_data['results']]), 2)
    avg_st_employment_percentage = round(statistics.mean([d.get('st_employment_percentage', 0) for d in merged_data['results']]), 2)
    avg_sc_employment_percentage = round(statistics.mean([d.get('sc_employment_percentage', 0) for d in merged_data['results']]), 2)
    avg_women_pd_percentage = round(statistics.mean([d.get('women_pd_percentage', 0) for d in merged_data['results']]), 2)
    avg_disabled_ratio = round(statistics.mean([d.get('disabled_ratio', 0) for d in merged_data['results']]), 2)
    avg_total_marks = round(statistics.mean([d.get('overall_total_marks', 0) for d in merged_data['results']]), 2)
    
    logger.info(f"Top district: {top_district['group_name']} with total marks {top_district.get('overall_total_marks', 0)}")
    logger.info(f"Bottom district: {bottom_district['group_name']} with total marks {bottom_district.get('overall_total_marks', 0)}")
    logger.info(f"State average total marks: {avg_total_marks}")
    
    return {
        "top_district": top_district,
        "bottom_district": bottom_district,
        "state_averages": {
            "hundred_days_percentage": avg_hundred_days_percentage,
            "st_employment_percentage": avg_st_employment_percentage,
            "sc_employment_percentage": avg_sc_employment_percentage,
            "women_pd_percentage": avg_women_pd_percentage,
            "disabled_ratio": avg_disabled_ratio,
            "total_marks": avg_total_marks
        },
        "district_ranks": district_ranks,
        "total_districts": len(sorted_districts)
    }

def process_district_category_data(merged_data):
    """
    Process district-level category employment data to summarize block information
    
    Args:
        merged_data (dict): Merged category and disabled employment data
    
    Returns:
        dict: Processed data with block information and district summary
    """
    if not merged_data or 'results' not in merged_data:
        logger.error("Invalid district category data format")
        return None
    
    logger.info(f"Processing district category data with {len(merged_data['results'])} blocks")
    
    # Format all data points to have 2 decimal places
    for block in merged_data['results']:
        for key, value in block.items():
            if isinstance(value, float):
                block[key] = round(value, 2)
                
        # Calculate percentages for each block similar to district calculations
        if 'hh_issued_jobcards_total' in block and block['hh_issued_jobcards_total'] > 0:
            block['hundred_days_percentage'] = round(block['families_completed_100_days_total'] / block['hh_issued_jobcards_total'] * 100, 2)
        else:
            block['hundred_days_percentage'] = 0
            
        if 'hh_issued_jobcards_sts' in block and block['hh_issued_jobcards_sts'] > 0:
            block['st_employment_percentage'] = round(block['no_of_hh_provided_employment_sts'] / block['hh_issued_jobcards_sts'] * 100, 2)
        else:
            block['st_employment_percentage'] = 0
            
        if 'hh_issued_jobcards_scs' in block and block['hh_issued_jobcards_scs'] > 0:
            block['sc_employment_percentage'] = round(block['no_of_hh_provided_employment_scs'] / block['hh_issued_jobcards_scs'] * 100, 2)
        else:
            block['sc_employment_percentage'] = 0
            
        if 'active_workers_women' in block and block['active_workers_women'] > 0:
            block['women_pd_percentage'] = round(block['no_of_persondays_generated_women'] / block['active_workers_women'] * 100, 2)
        else:
            block['women_pd_percentage'] = 0
    
    # Sort blocks by total marks (highest to lowest)
    sorted_blocks = sorted(merged_data['results'], key=lambda x: x.get('overall_total_marks', 0), reverse=True)
    
    # Calculate district averages
    avg_hundred_days_percentage = round(statistics.mean([b.get('hundred_days_percentage', 0) for b in merged_data['results']]), 2)
    avg_st_employment_percentage = round(statistics.mean([b.get('st_employment_percentage', 0) for b in merged_data['results']]), 2)
    avg_sc_employment_percentage = round(statistics.mean([b.get('sc_employment_percentage', 0) for b in merged_data['results']]), 2)
    avg_women_pd_percentage = round(statistics.mean([b.get('women_pd_percentage', 0) for b in merged_data['results']]), 2)
    avg_disabled_ratio = round(statistics.mean([b.get('disabled_ratio', 0) for b in merged_data['results']]), 2)
    avg_total_marks = round(statistics.mean([b.get('overall_total_marks', 0) for b in merged_data['results']]), 2)
    
    # Get highest and lowest performing blocks
    highest_block = sorted_blocks[0]
    lowest_block = sorted_blocks[-1]
    
    logger.info(f"Highest performing block: {highest_block['group_name']} with total marks {highest_block.get('overall_total_marks', 0)}")
    logger.info(f"Lowest performing block: {lowest_block['group_name']} with total marks {lowest_block.get('overall_total_marks', 0)}")
    logger.info(f"District average total marks: {avg_total_marks}")
    
    return {
        "blocks": sorted_blocks,
        "district_summary": {
            "total_blocks": len(merged_data['results']),
            "average_hundred_days_percentage": avg_hundred_days_percentage,
            "average_st_employment_percentage": avg_st_employment_percentage,
            "average_sc_employment_percentage": avg_sc_employment_percentage,
            "average_women_pd_percentage": avg_women_pd_percentage,
            "average_disabled_ratio": avg_disabled_ratio,
            "average_total_marks": avg_total_marks,
            "highest_performing_block": highest_block['group_name'],
            "highest_total_marks": highest_block.get('overall_total_marks', 0),
            "lowest_performing_block": lowest_block['group_name'],
            "lowest_total_marks": lowest_block.get('overall_total_marks', 0)
        }
    }

def generate_category_analysis(state_data, district_data, target_district):
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
Focus on the following metrics and their scoring criteria:

1. 100 days employment for households:
   - Less than 1%: 0 marks
   - 1% to 3%: 3 marks
   - More than 3%: 6 marks

2. Employment provided to ST workers:
   - Proportionate marks for ST participation with respect to ST job card holders (max 3 marks)

3. Employment provided to SC workers:
   - Proportionate marks for SC participation with respect to SC job card holders (max 3 marks)

4. Person days generation by women:
   - Full marks (7) if women person days ≥ 50% of total, otherwise proportionate marks

5. Person days generation by specially abled persons:
   - Full marks (3) if more than 2% of total person days, otherwise proportionate marks

Present your analysis in the following format:

<analysis>
<district_performance>
[Provide a 2-3 sentence analysis of the target district's performance compared to the top and bottom districts in the state. Highlight key strengths and weaknesses.]
</district_performance>

<inclusion_metrics>
[Provide a 2-3 sentence analysis focusing specifically on the district's performance in ST, SC, women, and disabled inclusion metrics.]
</inclusion_metrics>

<block_performance>
[Provide a 2-3 sentence analysis of the blocks within the target district, identifying the highest and lowest performers and any notable trends.]
</block_performance>

<recommendations>
[Offer 1-2 concise, data-driven recommendations for improving the target district's performance, particularly focusing on areas with lower marks. Note that our endevour is
to maximise there participation and our recommendations should include this urgency]
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
    logger.debug(f"Prompt to Claude for category analysis:\n{formatted_prompt}")
    
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
            thinking_file = f"category_thinking_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
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
        response_file = f"category_claude_response_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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
    Main function to fetch and process NREGS category employment data, then analyze it
    
    Args:
        date (str, optional): Date in YYYY-MM-DD format
        district (str, optional): District name for specific data
        output_format (str, optional): Output format ('text' or 'json')
    
    Returns:
        Union[str, dict]: Analysis result in specified format
    """
    logger.info(f"Starting NREGS category analysis for district: {district}, date: {date if date else 'current'}")
    
    # Use current date if none provided
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"Using current date: {date}")
    
    # Get state-level data from both APIs
    logger.info("Fetching state-level category employment data")
    state_category_data = get_category_employment_data(date)
    if not state_category_data:
        logger.error("Failed to get state-level category employment data")
        return None
    
    logger.info("Fetching state-level disabled employment data")
    state_disabled_data = get_disabled_employment_data(date)
    if not state_disabled_data:
        logger.error("Failed to get state-level disabled employment data")
        return None
    
    # Merge the state-level data
    merged_state_data = merge_category_and_disabled_data(state_category_data, state_disabled_data)
    if not merged_state_data:
        logger.error("Failed to merge state-level data")
        return None
    
    processed_state_data = process_state_category_data(merged_state_data)
    if not processed_state_data:
        logger.error("Failed to process state-level category data")
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
    
    # Get district-level data from both APIs
    logger.info(f"Fetching category employment data for district: {district}")
    district_category_data = get_category_employment_data(date, district)
    if not district_category_data:
        logger.error(f"Failed to get category employment data for district: {district}")
        return None
    
    logger.info(f"Fetching disabled employment data for district: {district}")
    district_disabled_data = get_disabled_employment_data(date, district)
    if not district_disabled_data:
        logger.error(f"Failed to get disabled employment data for district: {district}")
        return None
    
    # Merge the district-level data
    merged_district_data = merge_category_and_disabled_data(district_category_data, district_disabled_data)
    if not merged_district_data:
        logger.error(f"Failed to merge data for district: {district}")
        return None
    
    processed_district_data = process_district_category_data(merged_district_data)
    if not processed_district_data:
        logger.error(f"Failed to process category data for district: {district}")
        return None
    
    # Get district rank
    district_rank = processed_state_data["district_ranks"].get(district, None)
    total_districts = processed_state_data["total_districts"]
    
    # Find the district data in the state data for complete information
    target_district_data = None
    for d in merged_state_data['results']:
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
    logger.info("Generating category analysis using Claude 3.7")
    analysis = generate_category_analysis(
        result["state_data"], 
        result["district_data"], 
        district
    )
    
    # Create output based on requested format
    if output_format == "json":
        result["analysis"] = analysis
        
        # Save output to file
        filename = f"nregs_category_analysis_{district.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=4)
        logger.info(f"Analysis saved to {filename}")
        
        return result
    else:
        # Save output to file
        filename = f"nregs_category_analysis_{district.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(analysis)
        logger.info(f"Analysis saved to {filename}")
        
        return analysis

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='NREGS Category Employment Data Analysis')
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