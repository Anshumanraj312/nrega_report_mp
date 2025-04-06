import os
import sys
import json
import logging
import shutil
from datetime import datetime
import anthropic
import requests
from dotenv import load_dotenv

# Import all analysis modules
import labor_engagement
import avg_persondays
import category_employment
import work_management
import area_officer_inspection
import nmms_usage
import geotag_pending_works
import labour_material_ratio
import women_mate_engagement
import timely_payment
import zero_muster
import fra_beneficiaries

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nregs_comprehensive_report.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Base URL for the NREGA dashboard
BASE_URL = "https://dashboard.nregsmp.org"

# API endpoints
API_ENDPOINTS = [
    "/api/employment_workers/labour-engagement",
    "/api/employment_workers/avg-persondays",
    "/api/employment_workers/category-employment",
    "/api/employment_workers/disabled",
    "/api/employment_workers/transaction",
    "/api/employment_workers/work-management",
    "/api/employment_workers/recovery",
    "/api/employment_workers/inspection",
    "/api/employment_workers/nmms-usage",
    "/api/employment_workers/geotag-pending-works",
    "/api/employment_workers/labour-material-ratio",
    "/api/employment_workers/women-mate-engagement",
    "/api/employment_workers/timely-payment",
    "/api/employment_workers/zero-muster",
    "/api/employment_workers/fra-beneficiaries"
]

# Define output directories
OUTPUT_DIR = "output"
PDF_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "district_report_pdf")

def clean_string_for_comparison(s):
    """Remove all spaces and convert to lowercase for string comparison"""
    if s is None:
        return ""
    return "".join(s.lower().split())

def is_outlier_score(item, data_list, threshold_percentage=30):
    """
    Check if an item's score is significantly lower than the average (potential outlier)
    
    Args:
        item: The item to check
        data_list: List of all items
        threshold_percentage: How much below average to consider an outlier
        
    Returns:
        bool: True if the item is an outlier, False otherwise
    """
    if len(data_list) <= 1:
        return False
        
    # Calculate average score excluding this item
    other_items = [x for x in data_list if x.get("group_name") != item.get("group_name")]
    if not other_items:
        return False
        
    other_scores = [x.get("overall_total_marks", 0) for x in other_items]
    avg_score = sum(other_scores) / len(other_scores)
    
    # Check if item's score is significantly lower than average
    item_score = item.get("overall_total_marks", 0)
    threshold = avg_score * (1 - threshold_percentage/100)
    
    return item_score < threshold

def fetch_data(endpoint, params):
    """Fetch data from a specific API endpoint"""
    url = f"{BASE_URL}{endpoint}"
    try:
        logger.info(f"Fetching data from {url} with params {params}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error fetching data from {endpoint}: {e}")
        return {"results": []}

def combine_data(params):
    """Fetch and combine data from all endpoints with given parameters"""
    results_data = []
    for endpoint in API_ENDPOINTS:
        data = fetch_data(endpoint, params)
        results_data.append(data)
    
    # Merge results by group_name
    merged_map = {}
    for data_obj in results_data:
        if "results" in data_obj and isinstance(data_obj["results"], list):
            for item in data_obj["results"]:
                if "group_name" not in item:
                    continue
                
                group_name = item["group_name"]
                if group_name in merged_map:
                    merged_map[group_name].update(item)
                else:
                    merged_map[group_name] = dict(item)
    
    # Convert to list
    global_results = list(merged_map.values())
    
    # Calculate overall marks
    for item in global_results:
        # Extract values with default of 0 for None/missing values
        def safe_float(val):
            try:
                return float(val or 0)
            except (ValueError, TypeError):
                return 0
        
        # Calculate individual mark components
        labourMarks = safe_float(item.get("marks"))
        pdMarks = safe_float(item.get("pd_marks"))
        catMarks = safe_float(item.get("total_marks"))
        disabledMarks = safe_float(item.get("disabled_marks"))
        transactionMarks = safe_float(item.get("total_transaction_marks"))
        
        marksPrev = safe_float(item.get("marks_prev"))
        marksCurr = safe_float(item.get("marks_curr"))
        workManagementTotal = marksPrev + marksCurr
        
        inspectionMarks = safe_float(item.get("total_visit_marks"))
        pendingMarks = safe_float(item.get("pending_marks"))
        recoveryMarks = safe_float(item.get("recovery_marks"))
        nmmsMarks = safe_float(item.get("total_nmms_marks"))
        
        # Geotag marks
        geotagMarksPhase0 = safe_float(item.get("phase_0_assets_geotag_marks"))
        geotagMarksPhase1 = safe_float(item.get("phase_1_before_geotag_marks"))
        geotagMarksPhase2 = safe_float(item.get("phase_2_during_geotag_marks"))
        geotagMarksPhase3 = safe_float(item.get("phase_3_after_geotag_marks"))
        totalGeotagMarks = geotagMarksPhase0 + geotagMarksPhase1 + geotagMarksPhase2 + geotagMarksPhase3
        
        ratioMarks = safe_float(item.get("ratio_marks"))
        womenMateMarks = safe_float(item.get("women_mate_marks"))
        timelyPaymentMarks = safe_float(item.get("timely_payment_marks"))
        zeroMusterMarks = safe_float(item.get("zero_muster_marks"))
        fraMarks = safe_float(item.get("total_fra_marks"))
        
        # Update the item with calculated values
        item["overall_total_marks"] = round(
            labourMarks + pdMarks + catMarks + disabledMarks + transactionMarks +
            workManagementTotal + inspectionMarks + pendingMarks + recoveryMarks + 
            nmmsMarks + totalGeotagMarks + ratioMarks + womenMateMarks + 
            timelyPaymentMarks + zeroMusterMarks + fraMarks, 2
        )
        
        # Assign grade based on overall_total_marks
        if item["overall_total_marks"] >= 70:
            item["grade"] = "A"
        elif item["overall_total_marks"] >= 60:
            item["grade"] = "B"
        elif item["overall_total_marks"] >= 45:
            item["grade"] = "C"
        else:
            item["grade"] = "D"
    
    # Sort results by overall_total_marks in descending order
    global_results.sort(key=lambda x: x.get("overall_total_marks", 0), reverse=True)
    
    return global_results

def get_district_data(date):
    """Get all district data for a given date"""
    params = {"date": date}
    return combine_data(params)

def get_block_data(date, district):
    """Get all block data for a given district and date with improved filtering"""
    params = {"date": date, "district": district}
    data = combine_data(params)
    
    filtered_data = []
    district_name_clean = clean_string_for_comparison(district)
    
    # First pass: Filter out any rows that match the district name AND have null/zero workers
    for item in data:
        group_name = item.get("group_name", "")
        group_name_clean = clean_string_for_comparison(group_name)
        
        # Check for worker count
        total_workers = None
        worker_fields = ["registered_worker", "total_registered_workers", "Total Registered Workers"]
        for field in worker_fields:
            worker_count = item.get(field)
            if worker_count is not None:
                total_workers = worker_count
                break
                
        # Check if this item should be filtered out
        should_filter = False
        
        # Case 1: Name matches district AND zero/null workers
        if group_name_clean == district_name_clean and (total_workers is None or total_workers == 0):
            logger.info(f"Filtering out block {group_name} - Matches district name and has null/zero workers")
            should_filter = True
            
        # Keep the item if it shouldn't be filtered
        if not should_filter:
            filtered_data.append(item)
    
    # Second pass: Check for statistical outliers in score
    # Only do this if we have 5 or more blocks
    if len(filtered_data) >= 5:
        final_data = []
        for item in filtered_data:
            if is_outlier_score(item, filtered_data, threshold_percentage=40):
                logger.info(f"Filtering out block {item.get('group_name')} - Score is an outlier")
            else:
                final_data.append(item)
        return final_data
    
    return filtered_data

def get_panchayat_data(date, district, block):
    """Get all panchayat data for a given block, district, and date with improved filtering"""
    params = {"date": date, "district": district, "block": block}
    data = combine_data(params)
    
    filtered_data = []
    district_name_clean = clean_string_for_comparison(district)
    block_name_clean = clean_string_for_comparison(block)
    
    # First pass: Filter out any rows that match either the district or block name AND have null/zero workers
    for item in data:
        group_name = item.get("group_name", "")
        group_name_clean = clean_string_for_comparison(group_name)
        
        # Check for worker count
        total_workers = None
        worker_fields = ["registered_worker", "total_registered_workers", "Total Registered Workers"]
        for field in worker_fields:
            worker_count = item.get(field)
            if worker_count is not None:
                total_workers = worker_count
                break
                
        # Check if this item should be filtered out
        should_filter = False
        
        # Case 1: Name matches district AND zero/null workers
        if group_name_clean == district_name_clean and (total_workers is None or total_workers == 0):
            logger.info(f"Filtering out panchayat {group_name} - Matches district name and has null/zero workers")
            should_filter = True
            
        # Case 2: Name matches block AND zero/null workers
        elif group_name_clean == block_name_clean and (total_workers is None or total_workers == 0):
            logger.info(f"Filtering out panchayat {group_name} - Matches block name and has null/zero workers")
            should_filter = True
            
        # Keep the item if it shouldn't be filtered
        if not should_filter:
            filtered_data.append(item)
    
    # Second pass: Check for statistical outliers in score
    # Only do this if we have a significant number of panchayats
    if len(filtered_data) >= 10:
        final_data = []
        for item in filtered_data:
            if is_outlier_score(item, filtered_data, threshold_percentage=40):
                logger.info(f"Filtering out panchayat {item.get('group_name')} - Score is an outlier")
            else:
                final_data.append(item)
        return final_data
    
    return filtered_data

def extract_performance_data(data, level="District", top_n=5, bottom_n=5, is_panchayat=False):
    """Extract top N and bottom N performers from data"""
    # Extract relevant fields for display
    display_data = [{
        "name": item.get("group_name", "Unknown"),
        "marks": round(item.get("overall_total_marks", 0), 2),
        "grade": item.get("grade", ""),
        "maxMarks": 103
    } for item in data]
    
    # Get top N performers
    top_performers = display_data[:top_n]
    
    # For panchayats, drop bottom-most before selecting bottom 5
    if is_panchayat and len(display_data) > bottom_n + 1:
        # Skip the very bottom one and take the next 5
        bottom_performers = display_data[-(bottom_n+1):-1]
    else:
        # For non-panchayat data or if not enough data, just take bottom N
        bottom_performers = display_data[-bottom_n:]
    
    return {
        f"top{top_n}": top_performers,
        f"bottom{bottom_n}": bottom_performers
    }

def find_item_by_name(data, name):
    """Find an item in the data by its group_name"""
    for item in data:
        if item.get("group_name") == name:
            return item
    return None

# Add this function to your existing script
def calculate_state_average(district_data):
    """Calculate the state average score from all district data"""
    if not district_data or len(district_data) == 0:
        logger.warning("No district data available to calculate state average")
        return 0
    
    total_marks = sum(district.get("overall_total_marks", 0) for district in district_data)
    avg_marks = total_marks / len(district_data)
    
    return round(avg_marks, 2)

def create_performance_summary(district_data, selected_district=None, date=None):
    """Create a structured JSON with performance data at all levels"""
    # Calculate state average
    state_average = calculate_state_average(district_data)
    
    # Create the result object
    result = {
        "metadata": {
            "date": date,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "maxMarks": 103,
            "stateAverage": state_average  # Add state average to metadata
        },
        "districts": extract_performance_data(district_data, level="District")
    }
    
    # If a district is selected, add its data and blocks
    if selected_district:
        selected_district_data = find_item_by_name(district_data, selected_district)
        
        if selected_district_data:
            district_marks = round(selected_district_data.get("overall_total_marks", 0), 2)
            
            # Calculate difference from state average
            diff_from_state_avg = round(district_marks - state_average, 2)
            is_above_state_avg = district_marks > state_average
            
            result["selectedDistrict"] = {
                "name": selected_district,
                "marks": district_marks,
                "grade": selected_district_data.get("grade", ""),
                "maxMarks": 103,
                "comparedToStateAverage": {
                    "difference": diff_from_state_avg,
                    "isAbove": is_above_state_avg,
                    "stateAverage": state_average
                }
            }
            
            # Add district rank
            for idx, district in enumerate(district_data):
                if district.get("group_name") == selected_district:
                    result["selectedDistrict"]["rank"] = idx + 1
                    result["selectedDistrict"]["totalDistricts"] = len(district_data)
                    break
            
            # Get component-wise marks for the selected district
            if selected_district_data:
                result["selectedDistrict"]["componentMarks"] = {
                    "laborEngagement": selected_district_data.get("marks", 0),
                    "personDays": selected_district_data.get("pd_marks", 0),
                    "categoryEmployment": selected_district_data.get("total_marks", 0),
                    "disabledWorkers": selected_district_data.get("disabled_marks", 0),
                    "workManagement": selected_district_data.get("marks_prev", 0) + selected_district_data.get("marks_curr", 0),
                    "inspection": selected_district_data.get("total_visit_marks", 0),
                    "nmmsUsage": selected_district_data.get("total_nmms_marks", 0),
                    "geotagPendingWorks": (
                        selected_district_data.get("phase_0_assets_geotag_marks", 0) + 
                        selected_district_data.get("phase_1_before_geotag_marks", 0) + 
                        selected_district_data.get("phase_2_during_geotag_marks", 0) + 
                        selected_district_data.get("phase_3_after_geotag_marks", 0)
                    ),
                    "labourMaterialRatio": selected_district_data.get("ratio_marks", 0),
                    "womenMateEngagement": selected_district_data.get("women_mate_marks", 0),
                    "timelyPayment": selected_district_data.get("timely_payment_marks", 0),
                    "zeroMuster": selected_district_data.get("zero_muster_marks", 0),
                    "fraBeneficiaries": selected_district_data.get("total_fra_marks", 0)
                }
            
            # Get block-level data with improved filtering
            block_data = get_block_data(date, selected_district)
            
            # For each block, get panchayat data
            result["selectedDistrict"]["blockDetails"] = []
            
            for block_item in block_data:
                block_name = block_item.get("group_name")
                block_marks = round(block_item.get("overall_total_marks", 0), 2)
                
                # Calculate block's difference from state average
                block_diff_from_state_avg = round(block_marks - state_average, 2)
                block_is_above_state_avg = block_marks > state_average
                
                block_detail = {
                    "name": block_name,
                    "marks": block_marks,
                    "grade": block_item.get("grade", ""),
                    "maxMarks": 103,
                    "comparedToStateAverage": {
                        "difference": block_diff_from_state_avg,
                        "isAbove": block_is_above_state_avg,
                        "stateAverage": state_average
                    },
                    "componentMarks": {
                        "laborEngagement": block_item.get("marks", 0),
                        "personDays": block_item.get("pd_marks", 0),
                        "categoryEmployment": block_item.get("total_marks", 0),
                        "disabledWorkers": block_item.get("disabled_marks", 0),
                        "workManagement": block_item.get("marks_prev", 0) + block_item.get("marks_curr", 0),
                        "inspection": block_item.get("total_visit_marks", 0),
                        "nmmsUsage": block_item.get("total_nmms_marks", 0),
                        "geotagPendingWorks": (
                            block_item.get("phase_0_assets_geotag_marks", 0) + 
                            block_item.get("phase_1_before_geotag_marks", 0) + 
                            block_item.get("phase_2_during_geotag_marks", 0) + 
                            block_item.get("phase_3_after_geotag_marks", 0)
                        ),
                        "labourMaterialRatio": block_item.get("ratio_marks", 0),
                        "womenMateEngagement": block_item.get("women_mate_marks", 0),
                        "timelyPayment": block_item.get("timely_payment_marks", 0),
                        "zeroMuster": block_item.get("zero_muster_marks", 0),
                        "fraBeneficiaries": block_item.get("total_fra_marks", 0)
                    }
                }
                
                # Get panchayat data for this block with improved filtering
                panchayat_data = get_panchayat_data(date, selected_district, block_name)
                if panchayat_data:
                    block_detail["panchayats"] = extract_performance_data(
                        panchayat_data, level="Panchayat", is_panchayat=True)
                
                result["selectedDistrict"]["blockDetails"].append(block_detail)
            
            # Sort blocks by marks in descending order
            result["selectedDistrict"]["blockDetails"].sort(
                key=lambda x: x.get("marks", 0), reverse=True
            )
    
    return result


def generate_detailed_analysis(district, date, output_format="text"):
    """Run all individual analyses and combine them"""
    # Run all individual analyses
    labor_result = labor_engagement.main(date, district, "json")
    persondays_result = avg_persondays.main(date, district, "json")
    category_result = category_employment.main(date, district, "json")
    work_management_result = work_management.main(date, district, "json")
    inspection_result = area_officer_inspection.main(date, district, "json")
    nmms_result = nmms_usage.main(date, district, "json")
    geotag_result = geotag_pending_works.main(date, district, "json")
    labour_material_result = labour_material_ratio.main(date, district, "json")
    women_mate_result = women_mate_engagement.main(date, district, "json")
    timely_payment_result = timely_payment.main(date, district, "json")
    zero_muster_result = zero_muster.main(date, district, "json")
    fra_beneficiaries_result = fra_beneficiaries.main(date, district, "json")

    # Extract analysis text from results (ONLY the analysis text to save tokens)
    labor_analysis = labor_result.get("analysis", "") if labor_result else "Labor engagement analysis not available."
    persondays_analysis = persondays_result.get("analysis", "") if persondays_result else "Person days analysis not available."
    category_analysis = category_result.get("analysis", "") if category_result else "Category employment analysis not available."
    work_management_analysis = work_management_result.get("analysis", "") if work_management_result else "Work management analysis not available."
    inspection_analysis = inspection_result.get("analysis", "") if inspection_result else "Area officer inspection analysis not available."
    nmms_analysis = nmms_result.get("analysis", "") if nmms_result else "NMMS usage analysis not available."
    geotag_analysis = geotag_result.get("analysis", "") if geotag_result else "Geotag pending works analysis not available."
    labour_material_analysis = labour_material_result.get("analysis", "") if labour_material_result else "Labour material ratio analysis not available."
    women_mate_analysis = women_mate_result.get("analysis", "") if women_mate_result else "Women mate engagement analysis not available."
    timely_payment_analysis = timely_payment_result.get("analysis", "") if timely_payment_result else "Timely payment analysis not available."
    zero_muster_analysis = zero_muster_result.get("analysis", "") if zero_muster_result else "Zero muster analysis not available."
    fra_beneficiaries_analysis = fra_beneficiaries_result.get("analysis", "") if fra_beneficiaries_result else "FRA beneficiaries analysis not available."
    
    # Combine analysis results
    detailed_analysis = {
        "labor_engagement": labor_analysis,
        "person_days": persondays_analysis,
        "category_employment": category_analysis,
        "work_management": work_management_analysis,
        "area_officer_inspection": inspection_analysis,
        "nmms_usage": nmms_analysis,
        "geotag_pending_works": geotag_analysis,
        "labour_material_ratio": labour_material_analysis,
        "women_mate_engagement": women_mate_analysis,
        "timely_payment": timely_payment_analysis,
        "zero_muster": zero_muster_analysis,
        "fra_beneficiaries": fra_beneficiaries_analysis
    }
    
    return detailed_analysis

def generate_html_report(performance_summary, detailed_analysis, district, date):
    """
    Generate a comprehensive HTML report using the Claude API with streaming to handle long requests
    
    Args:
        performance_summary (dict): Performance summary data
        detailed_analysis (dict): Detailed analysis from individual modules
        district (str): District name
        date (str): Date for the report
    
    Returns:
        str: HTML report content
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        error_msg = "ANTHROPIC_API_KEY environment variable not set"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Create the prompt for Claude - optimized for design and PDF compatibility
    prompt = f"""
You are an expert data analyst and report designer for the National Rural Employment Guarantee Scheme (NREGS) in Madhya Pradesh, India.
The Data you are getting is a summary from many individual reports and you have to make sense of it to paint a complete picture of the district. The suggestions and analysis you are going to make will impact
millions of life, so your analysis should be crisp and to the point. you need to think on each of these and provide analysis of the district and its block, let me tell you all individual report summaries i am giving and what we are aiming for

        "labor_engagement" - ideally should be above state average
        "person_days" - ideally should be above state average
        "category_employment" - SC, ST and women should be employed in maximum quantity
        "work_management" - for older works it should be above 90% while for rest above state average
        "area_officer_inspection" - minimum 10 for both DPC and ADPC
        "nmms_usage" - 100% is ideal
        "geotag_pending_works" - above state average
        "labour_material_ratio" - ideally between 35-40 percent, lower ratio denotes pendancy in bill payment and higher denotes skewed priority towards material intensive work.
        "women_mate_engagement" - ideally above 50%
        "timely_payment" - 100%
        "zero_muster" - lesser the better
        "fra_beneficiaries" - above state average
        "disabled workers" - ideally above 2%

Create a comprehensive, visually appealing HTML report for {district} district based on data from {date} that follows the design specification below.

<performance_summary>
{json.dumps(performance_summary, indent=2)}
</performance_summary>

<detailed_analysis>
{json.dumps(detailed_analysis, indent=2)}
</detailed_analysis>

Follow this specific design pattern for optimal A2 PDF conversion and modern reactive styling:

1. Use a professional blue color scheme with:
   - Primary color: #0056a6 (deep blue)
   - Secondary color: #2D8CC0 (medium blue)
   - Accent color: #FF9933 (orange)
   - Background colors: white (#FFFFFF) and light gray (#F5F7FA)
   - Text colors: dark (#333333) and medium (#555555)
   - Success color: #28a745 (green)
   - Warning color: #ffc107 (yellow)
   - Danger color: #dc3545 (red)
   - Info color: #17a2b8 (teal)

2. Create a proper header with:
   - The Madhya Pradesh emblem logo at the top left using this URL: https://upload.wikimedia.org/wikipedia/commons/thumb/a/ae/Emblem_of_Madhya_Pradesh.svg/200px-Emblem_of_Madhya_Pradesh.svg.png
   - District name in large font (36pt)
   - Report date clearly shown
   - NREGS MP title with proper styling
   - Use flexbox for header alignment with space-between justification
   - Header height should be fixed at exactly 180px
   - Important: Add page-break-after: avoid to the header to prevent empty first page

3. For the Component Contribution section, implement a Score Table with Color Indicators :
   - Create a professional table with the following columns:
     * Component Name (left-aligned)
     * Score (showing X.XX / XX format)
     * % of Total (showing percentage contribution to total score)
     * Performance (color-coded indicator)
   - Apply this color coding for the Performance column indicators:
     * Blue gradient (#0056a6 to #2D8CC0): High performance (7.5+ marks)
     * Green gradient (#28a745 to #5cb85c): Above average (3.5-7.5 marks)
     * Yellow gradient (#ffc107 to #ffdb58): Average (1.5-3.5 marks)
     * Orange gradient (#fd7e14 to #ff9800): Below average (0.1-1.5 marks)
     * Red gradient (#dc3545 to #ff6b6b): Critical (0 marks)
   - Set appropriate text color for each indicator (white for blue, green, orange, and red; dark for yellow)
   - Use proper table styling with:
     * Bordered cells
     * Blue header with white text
     * Alternate row shading (#f9f9f9 for even rows)
     * Rounded corners for the table container
     * Adequate cell padding (16px 20px)
   - Sort components by score in descending order
   - Include a legend below the table showing what each color means
   
   Example structure:
   ```html
   <div class="card keep-together">
     <h3>Component Contribution to Total Score</h3>
     <div class="overflow-x-auto">
       <table class="min-w-full bg-white border border-gray-200">
         <thead class="bg-primary text-white">
           <tr>
             <th class="py-3 px-4 text-left">Component</th>
             <th class="py-3 px-4 text-left">Score</th>
             <th class="py-3 px-4 text-left">% of Total</th>
             <th class="py-3 px-4 text-left">Performance</th>
           </tr>
         </thead>
         <tbody>
           <!-- For each component, replace COMPONENT_NAME, SCORE, MAX_SCORE, PERCENTAGE, CATEGORY_COLOR, CATEGORY_TEXT_COLOR, and CATEGORY_NAME with actual values -->
           <tr class="bg-white">
             <td class="py-3 px-4 border-b border-gray-200 font-medium">COMPONENT_NAME</td>
             <td class="py-3 px-4 border-b border-gray-200">SCORE / MAX_SCORE</td>
             <td class="py-3 px-4 border-b border-gray-200">PERCENTAGE%</td>
             <td class="py-3 px-4 border-b border-gray-200">
               <span 
                 class="inline-block px-2 py-1 rounded text-sm"
                 style="background-color: CATEGORY_COLOR; color: CATEGORY_TEXT_COLOR"
               >
                 CATEGORY_NAME
               </span>
             </td>
           </tr>
           <!-- Repeat for all components -->
         </tbody>
       </table>
     </div>
     
     <div class="color-legend mt-4 flex flex-wrap gap-4 justify-center">
       <!-- Add all color indicators with appropriate colors -->
       <div class="legend-item flex items-center">
         <div class="color-box w-5 h-5 rounded mr-2" style="background: linear-gradient(135deg, #0056a6 0%, #2D8CC0 100%);"></div>
         <span>High (7.5+ marks)</span>
       </div>
       <!-- Add other legend items -->
     </div>
   </div>
   ```
4. For Block-wise Performance, create a table with state average comparison:
<!-- Block Comparison Table -->
<div class="card keep-together">
  <h3>Block Performance Comparison</h3>
  <div class="overflow-x-auto">
    <table class="w-full border-collapse">
      <thead class="bg-primary text-white">
        <tr>
          <th class="p-4 text-left border">Block</th>
          <th class="p-4 text-left border">Score</th>
          <th class="p-4 text-center border">Grade</th>
          <th class="p-4 text-left border">vs State Avg</th>
          <th class="p-4 text-left border">Performance</th>
        </tr>
      </thead>
      <tbody>
        {{#each performance_summary.selectedDistrict.blockDetails}}
        <tr>
          <td class="p-4 border font-medium">{{name}}</td>
          <td class="p-4 border">{{marks}}</td>
          <td class="p-4 border text-center">
            <div class="grade-badge grade-{{grade}}-gradient">{{grade}}</div>
          </td>
          <td class="p-4 border {{comparedToStateAverage.isAbove ? 'text-success' : 'text-danger'}}">
            {{comparedToStateAverage.isAbove ? '+' : ''}}{{comparedToStateAverage.difference}}
          </td>
          <td class="p-4 border">
            <span style="display: inline-block; padding: 5px 10px; border-radius: 5px; 
              background: {{#if (gt comparedToStateAverage.difference 10)}}
                linear-gradient(135deg, #0056a6 0%, #2D8CC0 100%)
              {{else if (gt comparedToStateAverage.difference 2)}}
                linear-gradient(135deg, #28a745 0%, #5cb85c 100%)
              {{else if (gt comparedToStateAverage.difference -2)}}
                linear-gradient(135deg, #ffc107 0%, #ffdb58 100%)
              {{else if (gt comparedToStateAverage.difference -10)}}
                linear-gradient(135deg, #fd7e14 0%, #ff9800 100%)
              {{else}}
                linear-gradient(135deg, #dc3545 0%, #ff6b6b 100%)
              {{/if}}; 
              color: {{#if (gt comparedToStateAverage.difference -2) and (lt comparedToStateAverage.difference 2)}}
                black
              {{else}}
                white
              {{/if}};">
              {{#if (gt comparedToStateAverage.difference 10)}}
                High
              {{else if (gt comparedToStateAverage.difference 2)}}
                Above Average
              {{else if (gt comparedToStateAverage.difference -2)}}
                Average
              {{else if (gt comparedToStateAverage.difference -10)}}
                Below Average
              {{else}}
                Critical
              {{/if}}
            </span>
          </td>
        </tr>
        {{/each}}
      </tbody>
    </table>
    <div class="text-right mt-2">
      <span class="inline-block px-2 py-1 bg-yellow-400 rounded text-sm font-bold">
        State Average: {{{performance_summary['metadata']['stateAverage']}}}
      </span>
    </div>
  </div>
</div>

<!-- Use these grade colors:
   - A: #28a745 (green)
   - B: #17a2b8 (blue)
   - C: #ffc107 (yellow, use black text)
   - D: #dc3545 (red)
   
   For the new columns, calculate these values:
   - [DIFFERENCE]: Format as "+X.XX" if block score is above state average, or "-X.XX" if below
   - [DIFFERENCE_CLASS]: "text-success" if block score is above state average, or "text-danger" if below
   - [PERFORMANCE_CATEGORY]: Based on the difference from state average:
     * "High" if 10+ points above state average
     * "Above Average" if 2-10 points above state average
     * "Average" if within ±2 points of state average
     * "Below Average" if 2-10 points below state average
     * "Critical" if 10+ points below state average
   - [PERFORMANCE_GRADIENT]: Color gradient based on performance category:
     * High: linear-gradient(135deg, #0056a6 0%, #2D8CC0 100%)
     * Above Average: linear-gradient(135deg, #28a745 0%, #5cb85c 100%)
     * Average: linear-gradient(135deg, #ffc107 0%, #ffdb58 100%)
     * Below Average: linear-gradient(135deg, #fd7e14 0%, #ff9800 100%)
     * Critical: linear-gradient(135deg, #dc3545 0%, #ff6b6b 100%)
   - [PERFORMANCE_TEXT_COLOR]: "white" for all except "Average" which should be "black"
-->

5. For tables and data presentation:
   - Use CSS Grid for all layout components
   - For top/bottom districts comparison, place tables in a perfect 2-column grid using grid-template-columns: 1fr 1fr
   - Ensure both tables in the district comparison row have equal height and width
   - Create a 40px gap between grid columns using the gap property
   - Use responsive tables with alternate row shading (#f9f9f9 for even rows)
   - Include proper spacing between columns and rows (padding: 16px 20px)
   - Use thin borders (#ddd) for separation
   - Add sticky headers with position: sticky for longer tables
   - Ensure tables have proper vertical alignment and text alignment
   - Set minimum font size of 14pt for all table content to ensure readability on A2 paper
   - Table headings should be at least 16pt
   - Wrap table containers in a div with class="keep-together" to prevent table breaks
   - Use border: 1px solid #e0e0e0 for table containers instead of box-shadow

6. For section organization:
   - Create clear visual separation between major sections using white cards with subtle borders (1px solid #e0e0e0) on a light gray background
   - Use consistent heading styles with bottom borders for section titles
   - Add page break hints for PDF rendering with the page-break-before property
   - Place best and worst performing block cards in a 2-column grid layout with equal widths
   - Use grid-template-columns: 1fr 1fr for the block comparison section
   - Ensure block comparison cards have identical heights with aligned elements
   - Use contrasting colors (#17a2b8 for best, #dc3545 for worst) to differentiate performance
   - Ensure sections have adequate padding (at least 40px) to prevent cramped appearance on A2
   - IMPORTANT: Place explicit page breaks BETWEEN sections, not at the beginning of them, like this:
     <!-- End of Executive Summary -->
     <div class="page-break"></div>
     <!-- Start of Component-wise Performance Section -->

7. For Block Analysis reports:
   - Place ALL Block Analysis reports in a 2-column grid layout using grid-template-columns: repeat(2, 1fr)
   - Each row should contain exactly 2 block analysis cards of equal height
   - Set fixed height for block analysis cards: min-height: 550px to ensure two fit perfectly on an A2 page
   - Maintain consistent padding (30px) and spacing between all block analysis cards
   - Use a left border accent (8px) in different colors to indicate performance level
   - Use border: 1px solid #e0e0e0 instead of box-shadow for clean floating design
   - IMPORTANT: Arrange block analysis cards in logical groups of 2 and place page breaks between them:
     
     <!-- First two blocks -->
     <div class="keep-together grid-2col">
       <!-- First two block analysis cards go here -->
     </div>
     
     <!-- Page break between block groups -->
     <div class="page-break"></div>
     
     <!-- Next two blocks -->
     <div class="keep-together grid-2col">
       <!-- Next two block analysis cards go here -->
     </div>
     
   - For the SWOT analysis in each block:
     * Focus on UNIQUE characteristics for each block rather than common elements
     * Highlight distinctive strengths and weaknesses that differentiate this block from others
     * Avoid redundancy between block analyses by emphasizing particular aspects:
       - For top-performing blocks: Focus on exceptional metrics and transferable practices
       - For middle-performing blocks: Highlight unique opportunities and specific challenges
       - For low-performing blocks: Emphasize critical intervention areas and unique threats
     * Use specific data points and percentages to support observations
     * Limit each SWOT category to 4-5 high-impact, distinctive points
   - Ensure all blocks maintain balanced analysis depth despite performance differences
   - Set SWOT container to min-height: 350px to ensure it stays together on page

8. For Panchayat Performance tables:
   - For each block, place heading as a single full-width element
   - Below each heading, place top 5 and bottom 5 panchayat tables side-by-side in ONE ROW using grid layout
   - Use grid-template-columns: 1fr 1fr for the tables row with 40px gap
   - Ensure both tables have equal width and aligned headers
   - Add key insights below both tables as a full-width element
   - Maintain consistent styling across all blocks' panchayat tables
   - Use border: 1px solid #e0e0e0 and border-radius: 12px instead of box-shadow
   - Scale table width to at least 45% of available width each (90% total)
   - IMPORTANT: Keep each block's panchayat section together as a unit:
   
     <div class="keep-together">
       <h3 class="panchayat-block-title">BLOCK NAME</h3>
       <div class="panchayat-tables">
         <!-- tables go here -->
       </div>
       <div class="panchayat-insights">
         <!-- insights go here -->
       </div>
     </div>
     
   - Add page breaks between block panchayat sections if they don't fit on current page:
   
     <!-- After each block's panchayat section, if needed -->
     <div class="page-break"></div>
   - IMPORTANT: Ensure that all blocks are covered and nothing missed

9. For specific A2 PDF optimization and clean floating design:
   - Use CSS @page rules for proper A2 page sizing with appropriate margins:
   
   @page {{
     size: A2 portrait;
     margin: 15mm;
   }}
   
   - Define exact A2 dimensions in print media queries with enhanced page break control:
   
   @media print {{
     html, body, div, span, applet, object, iframe, h1, h2, h3, h4, h5, h6 {{ 
       -webkit-print-color-adjust: exact !important; 
       print-color-adjust: exact !important;
       color-adjust: exact !important;
     }}
     body {{
       width: 420mm;  /* A2 width */
       height: 594mm; /* A2 height */
       margin: 0;
       padding: 0;
     }}
     .container {{
       max-width: none;
       width: 100%;
     }}
     /* Critical: Don't let these elements split across pages */
     .block-analysis-card, 
     .component-card, 
     .swot-container,
     .swot-box, 
     .recommendation-card,
     .treemap-item, 
     .chart-container, 
     table {{ 
       page-break-inside: avoid !important;
       break-inside: avoid !important;
     }}
     /* Control section breaks */
     section {{ 
       page-break-inside: avoid !important;
       break-inside: avoid !important;
     }}
     /* Handle explicit page breaks */
     .page-break {{ 
       page-break-before: always !important;
       break-before: always !important;
     }}
     /* Prevent orphaned headings */
     h2, h3 {{ 
       page-break-after: avoid !important;
       break-after: avoid !important; 
     }}
     /* Properly handle grid layouts */
     .grid-2col, .grid-3col, .panchayat-tables {{
       break-inside: avoid !important;
     }}
     /* Prevent header page break that causes empty first page */
     .report-header {{
       page-break-after: avoid !important;
       break-after: avoid !important;
     }}
   }}
   
   /* Remove shadows from all elements */
   .container,
   .card,
   .kpi-card,
   .block-card,
   .component-card,
   .block-analysis-card,
   .chart-container,
   .recommendation-card,
   .swot-box,
   .treemap-item {{
     box-shadow: none;
   }}
   
   /* Replace with subtle borders for visual separation */
   .card, 
   .kpi-card,
   .component-card,
   .block-analysis-card,
   .chart-container,
   .recommendation-card {{
     border: 1px solid #e0e0e0;
     border-radius: 12px;
   }}
   
   /* Ensure no filter shadows on grid elements */
   .grid-2col > div, 
   .grid-3col > div {{
     filter: none;
   }}
   
   /* Increase spacing between grid elements for better visual separation */
   .grid-2col,
   .grid-3col {{
     gap: 40px;
   }}
   
   /* Modern Grade Badge Styling */
   .grade-badge-large {{
     display: flex;
     align-items: center;
     justify-content: center;
     font-size: 36pt;
     font-weight: bold;
     width: 80px;
     height: 80px;
     border-radius: 16px;
     margin: 0 auto;
     box-shadow: 0 4px 12px rgba(0,0,0,0.1);
     color: white;
   }}

   .grade-badge {{
     display: inline-flex;
     align-items: center;
     justify-content: center;
     width: 40px;
     height: 40px;
     border-radius: 10px;
     font-weight: bold;
     font-size: 16pt;
     color: white;
     box-shadow: 0 2px 8px rgba(0,0,0,0.1);
   }}

   /* Grade Gradient Styles */
   .grade-A-gradient {{
     background: linear-gradient(135deg, #34c759 0%, #28a745 100%);
     color: white;
   }}

   .grade-B-gradient {{
     background: linear-gradient(135deg, #30b0c7 0%, #17a2b8 100%);
     color: white;
   }}

   .grade-C-gradient {{
     background: linear-gradient(135deg, #ffcc00 0%, #ffc107 100%);
     color: #333;
   }}

   .grade-D-gradient {{
     background: linear-gradient(135deg, #ff3b30 0%, #dc3545 100%);
     color: white;
   }}
   
   - Add these utility classes for explicit page break control:
   
   .keep-together {{
     page-break-inside: avoid !important;
     break-inside: avoid !important;
   }}
   
   .start-new-page {{
     page-break-before: always !important;
     break-before: always !important;
   }}
   
   .no-break-after {{
     page-break-after: avoid !important;
     break-after: avoid !important;
   }}
   
   - Use physical units (mm, cm) for critical dimensions to ensure print consistency
   - Add proper page-break-before: always classes at each logical section break
   - Ensure all content is sized proportionally for A2 paper (approximately 2x the size of A4)
   - Set base font size to 14pt (not pixels) with headings scaled proportionally
   - Use viewport meta tag to support proper scaling:
   
   <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=0, width=device-width">

10. Specific section layouts optimized for A2:
    - Executive Summary: Use CSS Grid for the entire layout with strategic page breaks
      * Ensure it fits approximately 2 complete A2 pages
      * Each SWOT box should have min-height: 200px
      * The component table should have width: 100% and appropriate spacing
    
    - Component-wise Performance: 
      * Use 3-column grid layout (grid-template-columns: repeat(3, 1fr))
      * Arrange exactly 3 components per row
      * Each component card should be min-height: 420px
      * This creates 4 rows of 3 components across 2-3 pages
    
    - Block-wise Performance: 
      * Table container should be min-height: 300px
      * Place exactly 2 block analysis cards per row
      * Each block card should be min-height: 550px
      * With 5 blocks total, this section should span 3 pages
    
    - Panchayat Performance: 
      * Each block's panchayat section should be kept together with class="keep-together"
      * If a block's panchayat tables don't fit on the current page, force a page break before it
    
    - Recommendations: 
      * All three recommendation cards should fit on a single page
      * Each card should be min-height: 550px

11. Typography improvements for A2 format:
    - Use a clean, professional sans-serif font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif
    - Set base font size to 14pt (not px) for body text to ensure readability on A2
    - Scale headings proportionally: h1 (32pt), h2 (28pt), h3 (22pt), h4 (18pt)
    - Use font-weight strategically (bold for headings and important data)
    - Maintain consistent line-height (1.6) for readability
    - Use color to highlight key metrics and insights
    - Ensure all font sizing uses point units (pt) for print consistency

12. A2-specific considerations:
    - Leave adequate whitespace between sections (minimum 50px vertical margins)
    - Avoid cramming too many elements into a single page - A2 allows for more generous spacing
    - Scale all interactive elements like buttons and controls to be at least 50% larger than standard web sizes
    - Use percentage-based widths for containers rather than fixed pixels to ensure proper scaling
    - Set all components to flex-grow appropriately to fill the A2 dimensions
    - Ensure all images and logos are high resolution (minimum 300dpi) for crisp printing on A2
    - Scale header and footer elements proportionally to the A2 format
    - To prevent container overflow and ensure proper page breaks, use these strict sizing rules:
      * Header height: exactly 180px
      * Section margins: exactly 50px top and bottom
      * Content cards: fixed heights based on A2 dimensions
      * Usable height per A2 page: ~550mm (excluding margins)
      * Deduct fixed elements (header, titles) from available space
      * Ensure content heights don't exceed remaining space

Each section should be cleanly separated with appropriate header styling and spacing. The document should have a professional, government report look while still being visually engaging and modern.

Include these exact sections:

1. Executive Summary - This should give a snapshot of the District. follow this exact pattern
- Begin with 4 different KPI cards arranged in a row:
  * District Rank (showing rank out of total districts)
  * Score (showing total points out of 103)
  * Grade (showing grade with appropriate color)
  * State Comparison - showing difference from state average

<div class="kpi-cards grid-4col">
  <!-- District Rank KPI Card -->
  <div class="kpi-card">
    <h3>District Rank</h3>
    <div class="kpi-value">
      {{{performance_summary['selectedDistrict']['rank']}}}
      <span style="font-size: 20pt;">/{{{performance_summary['selectedDistrict']['totalDistricts']}}}</span>
    </div>
    <div class="kpi-label">
      {{{{performance_summary['selectedDistrict']['rank'] <= performance_summary['selectedDistrict']['totalDistricts']/4 ? 
        'Top 25% of districts' : 
        performance_summary['selectedDistrict']['rank'] <= performance_summary['selectedDistrict']['totalDistricts']/2 ? 
        'Top 50% of districts' : 
        performance_summary['selectedDistrict']['rank'] <= performance_summary['selectedDistrict']['totalDistricts']*3/4 ? 
        'Bottom 50% of districts' : 
        'Bottom 25% of districts'}}}}
    </div>
  </div>
  
  <!-- Score KPI Card -->
  <div class="kpi-card">
    <h3>Total Score</h3>
    <div class="kpi-value">
      {{{performance_summary['selectedDistrict']['marks']}}}
      <span style="font-size: 20pt;">/103</span>
    </div>
    <div class="kpi-label">
      {{{{(performance_summary['selectedDistrict']['marks'] / 103 * 100).toFixed(1)}}}}% of maximum possible
    </div>
  </div>
  
  <!-- Grade KPI Card  -->
  <div class="kpi-card">
    <h3>Grade</h3>
    <div class="kpi-value">
      <div class="grade-badge-large grade-{{{performance_summary['selectedDistrict']['grade']}}}-gradient">
        {{{performance_summary['selectedDistrict']['grade']}}}
      </div>
    </div>
    <div class="kpi-label">
      {{{{performance_summary['selectedDistrict']['grade'] === 'A' ? 'Excellence' : 
          performance_summary['selectedDistrict']['grade'] === 'B' ? 'Good performance' : 
          performance_summary['selectedDistrict']['grade'] === 'C' ? 'Average performance' : 
          'Requires improvement'}}}}
    </div>
  </div>
  
  <!-- State Comparison KPI Card -->
  <div class="kpi-card">
    <h3>State Comparison</h3>
    <div class="kpi-value">
      <span class="{{{{performance_summary['selectedDistrict']['comparedToStateAverage']['isAbove'] ? 'text-success' : 'text-danger'}}}}">
        <span style="font-size: 36pt; margin-right: 5px;">{{{{performance_summary['selectedDistrict']['comparedToStateAverage']['isAbove'] ? '↑' : '↓'}}}}</span> {{{performance_summary['selectedDistrict']['comparedToStateAverage']['difference']}}}
      </span>
    </div>
    <div class="kpi-label">
      {{{{performance_summary['selectedDistrict']['comparedToStateAverage']['isAbove'] ? 'Above' : 'Below'}}}} state average: {{{performance_summary['metadata']['stateAverage']}}}
    </div>
  </div>

- 2 Tables in one grid with top 5/bottom 5 districts with their rank, score and grade as columns.
  Add a state average reference between these tables:
  
  <div class="grid-2col">
  <!-- Top 5 Districts table -->
  <div class="card">
    <h3>Top 5 Districts</h3>
    <table class="min-w-full bg-white border border-gray-200">
      <thead class="bg-primary text-white">
        <tr>
          <th class="py-3 px-4 text-left">District</th>
          <th class="py-3 px-4 text-left">Score</th>
          <th class="py-3 px-4 text-center">Grade</th>
        </tr>
      </thead>
      <tbody>
        {{#each performance_summary.districts.top5}}
        <tr class="{{#if @index % 2}}bg-white{{else}}bg-gray-50{{/if}}">
          <td class="py-3 px-4 border-b border-gray-200">{{name}}</td>
          <td class="py-3 px-4 border-b border-gray-200">{{marks}}</td>
          <td class="py-3 px-4 border-b border-gray-200 text-center">
            <div class="grade-badge grade-{{grade}}-gradient">{{grade}}</div>
          </td>
        </tr>
        {{/each}}
      </tbody>
    </table>
  </div>
  
  <!-- State average marker between tables -->
  <div class="state-average-marker text-center my-2">
    <span class="inline-block px-3 py-1 bg-yellow-400 rounded text-sm font-bold">
      State Average: {{{performance_summary['metadata']['stateAverage']}}}
    </span>
  </div>
  
  <!-- Bottom 5 Districts table -->
  <div class="card">
    <h3>Bottom 5 Districts</h3>
    <table class="min-w-full bg-white border border-gray-200">
      <thead class="bg-primary text-white">
        <tr>
          <th class="py-3 px-4 text-left">District</th>
          <th class="py-3 px-4 text-left">Score</th>
          <th class="py-3 px-4 text-center">Grade</th>
        </tr>
      </thead>
      <tbody>
        {{#each performance_summary.districts.bottom5}}
        <tr class="{{#if @index % 2}}bg-white{{else}}bg-gray-50{{/if}}">
          <td class="py-3 px-4 border-b border-gray-200">{{name}}</td>
          <td class="py-3 px-4 border-b border-gray-200">{{marks}}</td>
          <td class="py-3 px-4 border-b border-gray-200 text-center">
            <div class="grade-badge grade-{{grade}}-gradient">{{grade}}</div>
          </td>
        </tr>
        {{/each}}
      </tbody>
    </table>
  </div>
</div>

- A table showing components with score, percentage contribution, performance level, and state average comparison:
  
  ```html
  <div class="card keep-together">
    <h3>Component Contribution to Total Score</h3>
    <div class="overflow-x-auto">
      <table class="min-w-full bg-white border border-gray-200">
        <thead class="bg-primary text-white">
          <tr>
            <th class="py-3 px-4 text-left">Component</th>
            <th class="py-3 px-4 text-left">Score</th>
            <th class="py-3 px-4 text-left">% of Total</th>
            <th class="py-3 px-4 text-left">Performance</th>
            <th class="py-3 px-4 text-left">vs State Avg</th>
          </tr>
        </thead>
        <tbody>
          <!-- For each component, add vs State Avg column -->
          <tr>
            <!-- Existing columns -->
            <td class="py-3 px-4 border-b border-gray-200">
              <span class="{{{{componentValue > stateAvgForComponent ? 'text-success' : 'text-danger'}}}}">
                {{{{componentValue > stateAvgForComponent ? '+' : ''}}}}{{{{(componentValue - stateAvgForComponent)|round(2)}}}}
              </span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
  ```

- key strengths/weakness/opportunity/Threats comparison with state average context:
  When analyzing SWOT, include explicit references to state average:
  
  ```html
  <div class="swot-container grid-2col">
    <div class="swot-box swot-strengths">
      <h4>Strengths</h4>
      <ul>
        <li>Components performing significantly above state average (e.g., X component is Y points above)</li>
        <!-- Other strengths -->
      </ul>
    </div>
    
    <div class="swot-box swot-weaknesses">
      <h4>Weaknesses</h4>
      <ul>
        <li>Components performing significantly below state average (e.g., X component is Y points below)</li>
        <!-- Other weaknesses -->
      </ul>
    </div>
    
    <div class="swot-box swot-opportunities">
      <h4>Opportunities</h4>
      <ul>
        <li>Components just below state average that could be improved with focused effort</li>
        <!-- Other opportunities -->
      </ul>
    </div>
    
    <div class="swot-box swot-threats">
      <h4>Threats</h4>
      <ul>
        <li>Risk of falling below state average in components where the margin is narrow</li>
        <!-- Other threats -->
      </ul>
    </div>
  </div>
  ```
  important - Your response should contain data to validate your points.

- Best and Worst performing Block with state average comparison:
  
  ```html
  <div class="grid-2col">
    <div class="block-card best-block">
      <h4>Best Performing Block</h4>
      <div class="block-data">
        <p class="block-name">[BLOCK_NAME]</p>
        <p class="block-score">Score: [SCORE]</p>
        <p class="block-grade">Grade: [GRADE]</p>
        <p class="state-comparison">
          <span class="{{{{bestBlockScore > stateAverage ? 'text-success' : 'text-danger'}}}}">
            {{{{Math.abs(bestBlockScore - stateAverage)|round(2)}}}} points 
            {{{{bestBlockScore > stateAverage ? 'above' : 'below'}}}} state average
          </span>
        </p>
      </div>
    </div>
    
    <div class="block-card worst-block">
      <h4>Worst Performing Block</h4>
      <div class="block-data">
        <p class="block-name">[BLOCK_NAME]</p>
        <p class="block-score">Score: [SCORE]</p>
        <p class="block-grade">Grade: [GRADE]</p>
        <p class="state-comparison">
          <span class="{{{{worstBlockScore > stateAverage ? 'text-success' : 'text-danger'}}}}">
            {{{{Math.abs(worstBlockScore - stateAverage)|round(2)}}}} points 
            {{{{worstBlockScore > stateAverage ? 'above' : 'below'}}}} state average
          </span>
        </p>
      </div>
    </div>
  </div>
  ```

- Low hanging fruits where little effort can lead to better ranking, using state average as reference:
  
  ```html
  <div class="card low-hanging-fruits">
    <h3>Quick Win Opportunities</h3>
    <p>Components just below state average where targeted efforts could yield significant improvements:</p>
    <ul>
      <li>[COMPONENT_NAME]: Currently [COMPONENT_SCORE] ([DIFFERENCE] points below state average) - [IMPROVEMENT_SUGGESTION]</li>
      <!-- Add specific components that are just slightly below state average -->
    </ul>
    <p>Focus on these areas could efficiently improve overall district ranking.</p>
  </div>
  ```

- it should cover around 2-3 pdf pages.



2. Component-wise Performance - Analysis of all 13 parameters with scores, progress bars, and insights Your response should contain data to validate your points.
Include district ranking/score in that component, top districts, improvement potential and suggestions, blocks in the district who are performig good and bad. 
place three components in one row to optimise space.
3. Block-wise Performance - Start with a comparison table showing blocks with visual performance bars and state average marker. Then below it Show Comparison of all blocks with component scores and identified strengths/weaknesses. All these blocks are from same district, analyse there performance for SWOT.
Important: Do not leave out any Block from analysis.
4. Panchayat Performance - Top/bottom 5 performers for each block with analysis
5. Recommendations - Actionable suggestions in three categories: Priority Areas, Replicating Success, and Operational Improvements

For the footer, use only this exact content:
<div class="copyright">
    <p>&copy; Prepared By: Anshuman Raj (CEO ZP SIDHI)</p>
    <p>Data as of {date} | Report Generated on {datetime.now().strftime('%B %d, %Y at %H:%M:%S')}</p>
</div>

VERY IMPORTANT FOR PAGE BREAKS: To ensure proper PDF rendering with clean page breaks, follow these precise instructions:

1. Place the page break divs BETWEEN sections, not at the beginning of sections:
   <!-- End of Executive Summary -->
   <div class="page-break"></div>
   <!-- Start of Component-wise Performance -->

2. For the block analysis section, divide blocks into logical pairs with page breaks between them:
   <!-- First two blocks -->
   <div class="keep-together grid-2col">
     <!-- Block analysis for blocks 1-2 -->
   </div>
   <div class="page-break"></div>
   <!-- Next two blocks -->
   <div class="keep-together grid-2col">
     <!-- Block analysis for blocks 3-4 -->
   </div>
   <div class="page-break"></div>
   <!-- Final block (if odd number) -->
   <div class="keep-together">
     <!-- Block analysis for block 5 -->
   </div>

3. For panchayat sections, keep each block's panchayat content together and use page breaks between blocks:
   <div class="keep-together">
     <h3 class="panchayat-block-title">BLOCK 1</h3>
     <!-- Block 1 panchayat content -->
   </div>
   <div class="page-break"></div>
   <div class="keep-together">
     <h3 class="panchayat-block-title">BLOCK 2</h3>
     <!-- Block 2 panchayat content -->
   </div>

4. For component cards, ensure they're arranged in rows of exactly 3 with consistent heights:
   <div class="keep-together grid-3col">
     <!-- First row of 3 component cards -->
   </div>
   <div class="page-break"></div>
   <div class="keep-together grid-3col">
     <!-- Second row of 3 component cards -->
   </div>

Important: Create complete, valid HTML with all CSS inline to ensure consistent rendering across systems. The document will be converted to PDF using Playwright, so ensure all styling is PDF-compatible without any JavaScript.
Your response should be exhaustive and contain ample data to validate your points.
"""
    
    # Save the prompt to a text file for debugging
    prompt_filename = os.path.join(OUTPUT_DIR, f"claude_prompt_{district.lower()}_{date.replace('-', '')}.txt")
    try:
        os.makedirs(os.path.dirname(prompt_filename), exist_ok=True)
        with open(prompt_filename, 'w', encoding='utf-8') as f:
            f.write(prompt)
        logger.info(f"Saved Claude prompt to {prompt_filename}")
    except Exception as e:
        logger.error(f"Error saving Claude prompt: {str(e)}")


    logger.info("Generating HTML report using Claude 3.7 with streaming")
    
    try:
        client = anthropic.Anthropic(api_key=api_key)
        
        # Use streaming for the request to handle long generation
        html_content = ""
        
        # Create a streaming request
        with client.messages.stream(
            model="claude-3-7-sonnet-20250219",
            max_tokens=64000,
            thinking={
                "type": "enabled",
                "budget_tokens": 25000
            },
            messages=[{"role": "user", "content": prompt}]
        ) as stream:
            # Process the stream
            for chunk in stream.text_stream:
                # Append to our full content
                html_content += chunk
                
                # Log progress periodically
                if len(html_content) % 10000 == 0:
                    logger.info(f"Received {len(html_content)} characters of HTML report so far")
            
            # Get the final message after streaming completes
            response = stream.get_final_message()
            
            # Log token usage if available
            if hasattr(response, 'usage'):
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
                
                # Create output directory if it doesn't exist
                os.makedirs(OUTPUT_DIR, exist_ok=True)
                
                # Save thinking to file
                thinking_file = os.path.join(OUTPUT_DIR, f"report_thinking_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
                with open(thinking_file, 'w', encoding='utf-8') as f:
                    f.write(thinking_text)
                logger.info(f"Thinking output saved to {thinking_file}")
        
        # Extract HTML content from the response (look for complete HTML)
        if "<!DOCTYPE html>" in html_content:
            # Extract the html content if Claude has included other text
            start_idx = html_content.find("<!DOCTYPE html>")
            end_idx = html_content.rfind("</html>") + 7  # Include the closing tag
            if end_idx > start_idx:  # Valid HTML found
                html_content = html_content[start_idx:end_idx]
        
        logger.info(f"Successfully generated HTML report with {len(html_content)} characters")
        return html_content
    
    except Exception as e:
        logger.error(f"Error generating HTML report: {str(e)}")
        raise



def setup_playwright_linux():
    """
    Check and install required dependencies for Playwright on Linux
    Returns True if successful, False otherwise
    """
    import platform
    import subprocess
    import shutil
    
    # Only run on Linux
    if platform.system() != "Linux":
        return True
        
    logger.info("Checking Playwright dependencies for Linux...")
    
    # Check if we're running as root or have sudo privileges
    has_sudo = False
    try:
        # Check for sudo
        sudo_path = shutil.which("sudo")
        if sudo_path:
            sudo_test = subprocess.run(
                ["sudo", "-n", "true"], 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
            )
            has_sudo = sudo_test.returncode == 0
    except Exception:
        pass
        
    # Determine Linux distribution
    distro = ""
    try:
        # Try to get distribution info
        if os.path.exists("/etc/os-release"):
            with open("/etc/os-release", "r") as f:
                os_info = f.read()
                if "debian" in os_info.lower() or "ubuntu" in os_info.lower():
                    distro = "debian"
                elif "centos" in os_info.lower() or "fedora" in os_info.lower() or "rhel" in os_info.lower():
                    distro = "redhat"
    except Exception as e:
        logger.warning(f"Could not determine Linux distribution: {e}")
        
    # Check if Playwright is installed
    try:
        import playwright
        logger.info("Playwright is installed")
    except ImportError:
        logger.warning("Playwright not installed. Installing...")
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", "playwright"], check=True)
            logger.info("Playwright installed successfully")
        except Exception as e:
            logger.error(f"Failed to install Playwright: {e}")
            return False
            
    # Try to install Playwright browser
    try:
        logger.info("Installing Playwright Chromium browser...")
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
        logger.info("Playwright Chromium installed successfully")
    except Exception as e:
        logger.warning(f"Failed to install Playwright Chromium automatically: {e}")
        
        # Provide instructions based on distribution
        if distro == "debian":
            deps = "apt-get update && apt-get install -y libglib2.0-0 libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libdbus-1-3 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2"
        elif distro == "redhat":
            deps = "yum install -y alsa-lib atk at-spi2-atk at-spi2-core cups-libs dbus-libs expat GConf2 glib2 gtk3 libX11 libXcomposite libXcursor libXdamage libXext libXfixes libXi libXrandr libXScrnSaver libXtst nspr nss pango xorg-x11-server-Xvfb"
        else:
            deps = "Install Chromium dependencies for your distribution"
            
        if has_sudo:
            logger.warning(f"You may need to run: sudo {deps}")
        else:
            logger.warning(f"You may need to run as root: {deps}")
            
        logger.warning("After installing dependencies, run: python -m playwright install chromium")
        return False
        
    # Test if Playwright works by running a simple script
    try:
        logger.info("Testing Playwright installation...")
        test_script = """
import asyncio
from playwright.async_api import async_playwright

async def test_browser():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto('about:blank')
        title = await page.title()
        await browser.close()
        return title

print("Playwright test successful!")
"""
        subprocess.run([sys.executable, "-c", test_script], check=True)
        logger.info("Playwright is working correctly")
        return True
    except Exception as e:
        logger.error(f"Playwright test failed: {e}")
        logger.error("PDF generation with Playwright may not work")
        return False



def convert_with_playwright(html_file, pdf_file):
    """
    Convert HTML file to PDF using Playwright browser engine for high-fidelity rendering
    
    Args:
        html_file (str): Path to the HTML file
        pdf_file (str): Path where the PDF should be saved
        
    Returns:
        bool: True if conversion was successful, False otherwise
    """
    try:
        import asyncio
        from playwright.async_api import async_playwright
        
        async def convert():
            async with async_playwright() as p:
                # Launch browser with higher timeout
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                # Load HTML file with file:// protocol
                file_url = f"file://{os.path.abspath(html_file)}"
                await page.goto(file_url, wait_until="networkidle", timeout=60000)
                
                # Wait for any potential lazy-loaded content
                await page.wait_for_timeout(2000)
                
                # Use A2 paper size
                await page.pdf(
                    path=pdf_file,
                    format="A2",
                    print_background=True,  # Print background graphics
                    margin={"top": "15mm", "right": "15mm", "bottom": "15mm", "left": "15mm"},
                    scale=1.0  # No scaling to preserve layout quality
                )
                
                await browser.close()
                return True
        
        # Run async function
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(convert())
        
        if os.path.exists(pdf_file) and result:
            logger.info(f"Successfully converted '{html_file}' to '{pdf_file}' with Playwright")
            return True
        return False
    
    except ImportError:
        logger.warning("Playwright not available. Install with: pip install playwright")
        logger.warning("After installation, run: python -m playwright install chromium")
        return False
    except Exception as e:
        logger.error(f"Error converting HTML to PDF with Playwright: {str(e)}")
        return False


def convert_with_pdfkit(html_file, pdf_file, wkhtmltopdf_path=None):
    """
    Convert HTML file to PDF with high quality using pdfkit with enhanced settings
    
    Args:
        html_file (str): Path to the HTML file
        pdf_file (str): Path where the PDF should be saved
        wkhtmltopdf_path (str, optional): Path to wkhtmltopdf executable
        
    Returns:
        bool: True if conversion was successful, False otherwise
    """
    if not os.path.exists(html_file):
        logger.error(f"Error: HTML file '{html_file}' not found.")
        return False
    
    try:
        import pdfkit
        
        # Enhanced options for better quality
        options = {
            'page-size': 'A2',
            'orientation': 'Portrait',
            'margin-top': '15mm',
            'margin-right': '15mm',
            'margin-bottom': '15mm',
            'margin-left': '15mm',
            'encoding': 'UTF-8',
            'no-outline': None,
            'enable-local-file-access': None,
            'enable-javascript': None,
            'javascript-delay': '2000',  # Wait longer for JavaScript execution
            'print-media-type': None,    # Use print media CSS
            'disable-smart-shrinking': None,  # Preserve exact sizing
            'dpi': '300',                # Higher DPI for better quality
            'image-dpi': '300',          # Higher image DPI
            'image-quality': '100',      # Maximum image quality
            'zoom': '1.0',               # Normal zoom
            'footer-center': 'Page [page] of [topage]',
            'footer-font-size': '9',
            'quiet': None,
            # Force background rendering
            'background': None,
            # Use minimal font sizes
            'minimum-font-size': '8'
        }
        
        # Configure wkhtmltopdf path if provided
        config = None
        if wkhtmltopdf_path and os.path.exists(wkhtmltopdf_path):
            config = pdfkit.configuration(wkhtmltopdf=wkhtmltopdf_path)
        
        # Read HTML content and modify if needed to fix common rendering issues
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Force print styling into HTML content
        style_fix = '''
        <style>
        * {
            -webkit-print-color-adjust: exact !important;
            print-color-adjust: exact !important;
            color-adjust: exact !important;
        }
        
        @media print {
            body { background-color: white !important; }
            .card, .progress-bar, div { page-break-inside: avoid !important; }
            table { border-collapse: collapse !important; }
            table, th, td { border: 1px solid #ddd !important; }
            tr:nth-child(even) { background-color: #f9f9f9 !important; }
            img { max-width: 100% !important; }
        }
        </style>
        '''
        
        # Add style fix to head
        if '<head>' in html_content:
            html_content = html_content.replace('<head>', f'<head>{style_fix}')
            
            # Write modified content to a temporary file
            temp_html_file = html_file.replace('.html', '_temp.html')
            with open(temp_html_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            # Use the modified file for conversion
            html_file = temp_html_file
        
        # Convert HTML to PDF
        if config:
            pdfkit.from_file(html_file, pdf_file, options=options, configuration=config)
        else:
            pdfkit.from_file(html_file, pdf_file, options=options)
        
        # Delete temp file if it was created
        if html_file.endswith('_temp.html') and os.path.exists(html_file):
            try:
                os.remove(html_file)
            except Exception as e:
                logger.warning(f"Failed to delete temporary HTML file: {e}")
        
        if os.path.exists(pdf_file):
            logger.info(f"Successfully converted '{html_file}' to '{pdf_file}' with enhanced quality using pdfkit")
            return True
        return False
    except Exception as e:
        logger.error(f"Error converting HTML to PDF with pdfkit: {str(e)}")
        return False
    

def clean_up_files(html_filename, pdf_filename=None):
    """
    Clean up temporary files while preserving HTML and PDF reports
    
    Args:
        html_filename (str): Path to the HTML file to keep
        pdf_filename (str, optional): Path to the PDF file to keep
    """
    try:
        logger.info("Cleaning up temporary files...")
        
        # Files to keep (basename without path)
        files_to_keep = [os.path.basename(html_filename)]
        if pdf_filename and os.path.exists(pdf_filename):
            files_to_keep.append(os.path.basename(pdf_filename))
        
        # Extensions to delete
        extensions_to_delete = ['.log', '.json', '.txt', '.tmp', '.bak']
        
        # Clean main directory
        for filename in os.listdir('.'):
            # Skip directories
            if os.path.isdir(os.path.join('.', filename)) and filename != OUTPUT_DIR:
                continue
                
            # Check if file should be deleted based on extension
            file_ext = os.path.splitext(filename)[1].lower()
            if file_ext in extensions_to_delete or filename.endswith('_analysis.log'):
                try:
                    os.remove(filename)
                    logger.info(f"Deleted file: {filename}")
                except Exception as e:
                    logger.warning(f"Failed to delete file {filename}: {e}")
        
        # Clean output directory
        output_dir = os.path.dirname(html_filename)
        if os.path.exists(output_dir) and os.path.isdir(output_dir):
            for filename in os.listdir(output_dir):
                filepath = os.path.join(output_dir, filename)
                
                # Skip directories and files we want to keep
                if os.path.isdir(filepath) or filename in files_to_keep:
                    continue
                
                # Delete log, JSON, and text files
                file_ext = os.path.splitext(filename)[1].lower()
                if file_ext in extensions_to_delete:
                    try:
                        os.remove(filepath)
                        logger.info(f"Deleted file from output dir: {filename}")
                    except Exception as e:
                        logger.warning(f"Failed to delete file {filename}: {e}")
        
        # Clean any nregs_analysis.log files in any subdirectories recursively
        for root, dirs, files in os.walk('.'):
            for filename in files:
                if filename.endswith('_analysis.log') or filename == 'nregs_analysis.log':
                    filepath = os.path.join(root, filename)
                    try:
                        os.remove(filepath)
                        logger.info(f"Deleted log file: {filepath}")
                    except Exception as e:
                        logger.warning(f"Failed to delete file {filepath}: {e}")
        
        logger.info("Cleanup completed successfully")
        
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        print(f"Warning: Cleanup failed - {str(e)}")




def generate_pdf_from_html(html_filename, district, date):
    """
    Generate PDF from HTML file using only Playwright
    
    Args:
        html_filename (str): Path to HTML file
        district (str): District name
        date (str): Date string
    
    Returns:
        str: Path to generated PDF file
    """
    logger.info(f"Converting HTML to PDF using Playwright: {html_filename}")
    
    # Create PDF output directory if it doesn't exist
    os.makedirs(PDF_OUTPUT_DIR, exist_ok=True)
    
    # Create PDF filename
    pdf_basename = os.path.basename(html_filename).replace('.html', '')
    pdf_filename = os.path.join(PDF_OUTPUT_DIR, f"{pdf_basename}.pdf")
    
    try:
        logger.info("Generating PDF with Playwright (browser engine)...")
        
        import asyncio
        from playwright.async_api import async_playwright
        
        async def convert():
            async with async_playwright() as p:
                # Launch browser with higher timeout
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                # Load HTML file with file:// protocol
                file_url = f"file://{os.path.abspath(html_filename)}"
                await page.goto(file_url, wait_until="networkidle", timeout=60000)
                
                # Wait for any potential lazy-loaded content
                await page.wait_for_timeout(2000)
                
                # Use A2 paper size
                await page.pdf(
                    path=pdf_filename,
                    format="A2",
                    print_background=True,  # Print background graphics
                    margin={"top": "15mm", "right": "15mm", "bottom": "15mm", "left": "15mm"},
                    scale=1.0  # No scaling to preserve layout quality
                )
                
                await browser.close()
                return True
        
        # Run async function
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(convert())
        
        if os.path.exists(pdf_filename) and result:
            logger.info(f"Successfully converted '{html_filename}' to '{pdf_filename}' with Playwright")
            return pdf_filename
        else:
            raise Exception("PDF file was not created successfully")
    
    except ImportError:
        error_msg = "Playwright not available. Install with: pip install playwright"
        logger.error(error_msg)
        logger.error("After installation, run: python -m playwright install chromium")
        raise ImportError(error_msg)
    except Exception as e:
        error_msg = f"Error converting HTML to PDF with Playwright: {str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)
    

def main():
    # Check command line arguments
    if len(sys.argv) < 3:
        print("Usage: python generate_comprehensive_report.py <date> <district>")
        print("Example: python generate_comprehensive_report.py 2025-03-19 ANUPPUR")
        sys.exit(1)
    
    date = sys.argv[1]
    district = sys.argv[2]
    
    try:
        # Create output directories if they don't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        os.makedirs(PDF_OUTPUT_DIR, exist_ok=True)
        
        # Set up Playwright for Linux if needed
        setup_playwright_linux()
        
        # Get district and ranking data
        logger.info(f"Fetching performance data for date: {date}, district: {district}")
        district_data = get_district_data(date)
        performance_summary = create_performance_summary(district_data, district, date)
        
        # Get detailed analysis from all modules
        logger.info(f"Generating detailed analysis for district: {district}")
        detailed_analysis = generate_detailed_analysis(district, date)
        
        # Generate HTML report using Claude
        logger.info("Generating comprehensive HTML report")
        html_report = generate_html_report(performance_summary, detailed_analysis, district, date)
        
        # Save HTML report to file
        html_filename = os.path.join(OUTPUT_DIR, f"nregs_comprehensive_report_{district.lower()}_{date.replace('-', '')}.html")
        with open(html_filename, 'w', encoding='utf-8') as f:
            f.write(html_report)
        
        logger.info(f"Comprehensive report successfully saved to {html_filename}")
        
        # Generate PDF from HTML using only Playwright
        pdf_filename = None
        try:
            logger.info("Converting HTML to PDF using Playwright...")
            pdf_filename = generate_pdf_from_html(html_filename, district, date)
            print(f"\nComprehensive report successfully generated and saved to:")
            print(f"- HTML: {html_filename}")
            print(f"- PDF: {pdf_filename}")
        except Exception as pdf_error:
            logger.error(f"Error generating PDF: {str(pdf_error)}")
            print(f"\nComprehensive report successfully generated and saved to: {html_filename}")
            print(f"PDF generation failed: {str(pdf_error)}")
            print("Please install playwright to enable PDF generation:")
            print("  pip install playwright")
            print("  python -m playwright install chromium")
        
        # Clean up temporary files, keeping only the final HTML and PDF
        clean_up_files(html_filename, pdf_filename)
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        print(f"Error: {str(e)}")
if __name__ == "__main__":
    main()