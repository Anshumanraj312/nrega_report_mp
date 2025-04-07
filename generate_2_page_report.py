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
    Generate a concise HTML report using the Claude API with streaming to handle long requests
    
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

    # Convert data to JSON strings for prompt
    performance_summary_json = json.dumps(performance_summary, indent=2)
    detailed_analysis_json = json.dumps(detailed_analysis, indent=2)
    current_time = datetime.now().strftime('%B %d, %Y at %H:%M:%S')

    # Create the prompt for Claude - optimized for concise design with enhanced UI elements
    prompt = f"""
You are an expert data analyst and report designer for NREGS MP, skilled at creating concise yet insightful reports.
The data provided is a summary. Your task is to create a **highly focused and concise** HTML report (targeting **2-3 A2 pages**) for {district} district based on data from {date}. Prioritize relevance and actionable insights over exhaustive detail. The recommendations section, however, should remain comprehensive.

**Goal:** Generate a visually appealing report using the specified A2 layout and design template, but keep the content streamlined to fit the target length. Summarize effectively and avoid lengthy descriptions unless highlighting critical issues or successes.

<performance_summary>
{performance_summary_json}
</performance_summary>

<detailed_analysis>
{detailed_analysis_json}
</detailed_analysis>

**Adhere strictly to this design pattern and A2 PDF optimization guidelines:**

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
   - Header height should be fixed at exactly 150px to save space
   - Important: Add page-break-after: avoid to the header to prevent empty first page

3. For the Component Contribution section, implement a Score Table with enhanced Performance Capsules:
   - Create a professional table with the following columns:
     * Component Name (left-aligned)
     * Score (showing X.XX / XX format)
     * Top District (in that category)
     * Rank (in that category)
     * Performance (modern capsule-style indicators)
   - Apply this modern design for the Performance capsules:
     * High: Linear gradient from #0056a6 to #2D8CC0 with subtle glow effect
     * Above Avg: Linear gradient from #28a745 to #5cb85c with subtle glow effect
     * Average: Linear gradient from #ffc107 to #ffdb58
     * Below Avg: Linear gradient from #fd7e14 to #ff9800
     * Critical: Linear gradient from #dc3545 to #ff6b6b
   - Add subtle icons to capsules for better visual cues:
     * High: ↑ or ★
     * Above Avg: ↗
     * Average: →
     * Below Avg: ↘
     * Critical: ↓ or ⚠
   - Use appropriate text color for each capsule
   - Add a subtle shadow effect to make capsules appear slightly raised
   - Use proper table styling with:
     * Thin bordered cells
     * Blue header with white text
     * Alternate row shading (#f9f9f9 for even rows)
     * Rounded corners (8px) for the table container
     * Compact cell padding (12px 16px) to save space
   - Sort components by score in descending order
   - Include a compact legend below the table

4. For Block-wise Performance, create a space-efficient table:
   - Use the same enhanced capsule styling for performance levels
   - Include columns for Block, Score, Grade, vs State Avg, and Performance
   - Design grade badges with modern styling:
     * A: Linear gradient from #34c759 to #28a745
     * B: Linear gradient from #30b0c7 to #17a2b8
     * C: Linear gradient from #ffcc00 to #ffc107
     * D: Linear gradient from #ff3b30 to #dc3545
   - Use subtle hover effects for table rows
   - Implement compact row heights with adequate text spacing

5. For tables and data presentation:
   - Use CSS Grid for all layout components
   - Create responsive tables with subtle alternate row shading
   - Reduce spacing between columns and rows for compactness
   - Use thin borders (#ddd) for subtle separation
   - Add sticky headers with position: sticky for longer tables
   - Ensure tables have proper vertical alignment and text alignment
   - Set minimum font size of 14pt for all table content
   - Wrap table containers in a div with class="keep-together" to prevent table breaks
   - Use border: 1px solid #e0e0e0 for table containers

6. For section organization:
   - Optimize vertical spacing between sections to minimize empty space
   - Use subtle section dividers instead of excessive padding
   - Ensure content flows efficiently across the page
   - Use consistent heading styles with minimal vertical space consumption
   - IMPORTANT: Use minimal page breaks - only between major sections when absolutely necessary

7. For specific A2 PDF optimization and clean floating design:
   - Use CSS @page rules for proper A2 page sizing with appropriate margins:
   
   ```css
   @page {{
     size: A2 portrait;
     margin: 15mm;
   }}
   
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
       font-size: 14pt; /* Base font size for A2 */
     }}
     .container {{
       max-width: none;
       width: 100%;
     }}
     /* Critical: Don't let these elements split across pages */
     .card, .kpi-card, .block-card, .component-card, .swot-container, .swot-box,
     .recommendation-card, .treemap-item, .chart-container, table, .panchayat-tables, .panchayat-insights {{ 
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
       height: 0;
       overflow: hidden;
     }}
     /* Prevent orphaned headings */
     h2, h3, h4 {{ 
       page-break-after: avoid !important;
       break-after: avoid !important; 
     }}
     /* Properly handle grid layouts */
     .grid-2col, .grid-3col, .grid-4col, .panchayat-tables {{
       break-inside: avoid !important;
     }}
     /* Prevent header page break that causes empty first page */
     .report-header {{
       page-break-after: avoid !important;
       break-after: avoid !important;
     }}
     /* Scale font sizes for A2 */
     h1 {{ font-size: 32pt; }}
     h2 {{ font-size: 28pt; }}
     h3 {{ font-size: 22pt; }}
     h4 {{ font-size: 18pt; }}
     .kpi-value {{ font-size: 30pt; }} /* Adjust KPI font size */
     .kpi-value span {{ font-size: 20pt; }}
     .grade-badge-large {{ font-size: 36pt; width: 80px; height: 80px; border-radius: 16px; }}
     .grade-badge {{ font-size: 16pt; width: 40px; height: 40px; border-radius: 10px; }}
     table {{ font-size: 14pt; }} /* Ensure table text is readable */
     th {{ font-size: 16pt; }} /* Table headers */
   }}
   ```

   - Add these utility classes for styling and layout:
   
   ```css
   /* Remove shadows from all elements */
   .container, .card, .kpi-card, .block-card, .component-card,
   .block-analysis-card, .chart-container, .recommendation-card,
   .swot-box, .treemap-item {{
     box-shadow: none;
   }}

   /* Replace with subtle borders for visual separation */
   .card, .kpi-card, .component-card, .block-analysis-card,
   .chart-container, .recommendation-card, .block-card {{
     border: 1px solid #e0e0e0;
     border-radius: 12px;
     padding: 20px; /* Reduced padding for space efficiency */
     margin-bottom: 25px; /* Reduced spacing between cards */
   }}

   /* Grid layouts */
   .grid-2col, .grid-3col, .grid-4col {{
     display: grid;
     gap: 25px; /* Reduced gap for space efficiency */
   }}
   .grid-2col {{ grid-template-columns: repeat(2, 1fr); }}
   .grid-3col {{ grid-template-columns: repeat(3, 1fr); }}
   .grid-4col {{ grid-template-columns: repeat(4, 1fr); }}

   /* Modern Grade Badge Styling */
   .grade-badge-large {{
     display: inline-flex; 
     align-items: center;
     justify-content: center;
     font-size: 36pt;
     font-weight: bold;
     width: 80px;
     height: 80px;
     border-radius: 16px;
     box-shadow: 0 4px 12px rgba(0,0,0,0.1);
     color: white;
     vertical-align: middle;
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
     vertical-align: middle;
   }}

   /* Grade Gradient Styles */
   .grade-A-gradient {{ background: linear-gradient(135deg, #34c759 0%, #28a745 100%); color: white; }}
   .grade-B-gradient {{ background: linear-gradient(135deg, #30b0c7 0%, #17a2b8 100%); color: white; }}
   .grade-C-gradient {{ background: linear-gradient(135deg, #ffcc00 0%, #ffc107 100%); color: #333; }}
   .grade-D-gradient {{ background: linear-gradient(135deg, #ff3b30 0%, #dc3545 100%); color: white; }}

   /* Modern Performance Capsules */
   .performance-capsule {{
     display: inline-flex;
     align-items: center;
     padding: 6px 10px;
     border-radius: 30px;
     font-weight: 600;
     font-size: 14pt;
     box-shadow: 0 2px 5px rgba(0,0,0,0.08);
     min-width: 120px;
     justify-content: center;
     white-space: nowrap;
   }}

   .performance-capsule i {{
     margin-right: 5px;
     font-size: 12pt;
   }}

   .capsule-high {{
     background: linear-gradient(135deg, #0056a6, #2D8CC0);
     color: white;
     border: 1px solid rgba(255,255,255,0.2);
   }}

   .capsule-above {{
     background: linear-gradient(135deg, #28a745, #5cb85c);
     color: white;
     border: 1px solid rgba(255,255,255,0.2);
   }}

   .capsule-average {{
     background: linear-gradient(135deg, #ffc107, #ffdb58);
     color: #333;
     border: 1px solid rgba(0,0,0,0.1);
   }}

   .capsule-below {{
     background: linear-gradient(135deg, #fd7e14, #ff9800);
     color: white;
     border: 1px solid rgba(255,255,255,0.2);
   }}

   .capsule-critical {{
     background: linear-gradient(135deg, #dc3545, #ff6b6b);
     color: white;
     border: 1px solid rgba(255,255,255,0.2);
   }}

   /* Utility classes */
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
   
   /* Text colors */
   .text-success {{ color: #28a745; }}
   .text-danger {{ color: #dc3545; }}
   .text-warning {{ color: #ffc107; }}
   .text-info {{ color: #17a2b8; }}
   
   /* Background colors */
   .bg-primary {{ background-color: #0056a6; color: white; }}
   .bg-secondary {{ background-color: #2D8CC0; color: white; }}
   .bg-accent {{ background-color: #FF9933; color: white; }}
   .bg-light {{ background-color: #F5F7FA; }}
   
   /* SWOT styling */
   .swot-container {{
     display: grid;
     grid-template-columns: repeat(2, 1fr);
     gap: 20px;
     margin: 25px 0;
   }}
   
   .swot-box {{
     border: 1px solid #e0e0e0;
     border-radius: 12px;
     padding: 15px;
   }}
   
   .swot-box h4 {{
     margin-top: 0;
     margin-bottom: 10px;
     padding-bottom: 8px;
     border-bottom: 1px solid #eee;
     font-size: 16pt;
   }}
   
   .swot-box ul {{
     margin: 0;
     padding-left: 20px;
   }}
   
   .swot-box li {{
     margin-bottom: 8px;
     line-height: 1.4;
   }}
   
   .swot-strengths {{ border-left: 8px solid #28a745; }}
   .swot-weaknesses {{ border-left: 8px solid #dc3545; }}
   .swot-opportunities {{ border-left: 8px solid #17a2b8; }}
   .swot-threats {{ border-left: 8px solid #ffc107; }}
   
   /* KPI cards */
   .kpi-cards {{
     display: grid;
     grid-template-columns: repeat(4, 1fr);
     gap: 18px;
     margin-bottom: 25px;
   }}
   
   .kpi-card {{
     background-color: white;
     border-radius: 12px;
     padding: 18px;
     text-align: center;
     border: 1px solid #e0e0e0;
     transition: transform 0.2s;
   }}
   
   .kpi-card h3 {{
     color: #0056a6;
     margin-top: 0;
     margin-bottom: 8px;
     font-size: 18pt;
   }}
   
   .kpi-value {{
     font-size: 36pt;
     font-weight: bold;
     margin: 12px 0;
   }}
   
   .kpi-label {{
     font-size: 14pt;
     color: #555555;
   }}
   
   /* Modern District Spectrum styling */
   .district-spectrum-container {{
     margin: 25px 0;
     padding: 20px;
     border-radius: 12px;
     background-color: #fafafa;
     border: 1px solid #e0e0e0;
   }}

   .district-spectrum-title {{
     margin-top: 0;
     margin-bottom: 15px;
     font-size: 20pt;
     color: #0056a6;
   }}
   
   .district-spectrum-scale {{
     position: relative;
     height: 12px;
     margin: 30px 0;
     border-radius: 6px;
     background: linear-gradient(to right, 
       #dc3545 0%, 
       #ffc107 40%, 
       #28a745 80%);
     box-shadow: inset 0 1px 3px rgba(0,0,0,0.15);
     overflow: visible;
   }}
   
   .district-marker {{
     position: absolute;
     top: -14px;
     width: 40px;
     height: 40px;
     transform: translateX(-50%);
     z-index: 2;
   }}
   
   .marker-icon {{
     position: absolute;
     width: 24px;
     height: 24px;
     background-color: #0056a6;
     border: 3px solid white;
     border-radius: 50%;
     box-shadow: 0 2px 6px rgba(0,0,0,0.3);
     top: 0;
     left: 8px;
   }}
   
   .marker-line {{
     position: absolute;
     width: 2px;
     height: 16px;
     background-color: #0056a6;
     top: 24px;
     left: 19px;
   }}
   
   .district-marker-label {{
     position: absolute;
     top: -45px;
     left: 50%;
     transform: translateX(-50%);
     background-color: #0056a6;
     color: white;
     padding: 6px 12px;
     border-radius: 6px;
     white-space: nowrap;
     font-weight: bold;
     font-size: 13pt;
     box-shadow: 0 2px 8px rgba(0,0,0,0.2);
   }}
   
   .district-marker-label::after {{
     content: "";
     position: absolute;
     bottom: -6px;
     left: 50%;
     transform: translateX(-50%);
     width: 0;
     height: 0;
     border-left: 6px solid transparent;
     border-right: 6px solid transparent;
     border-top: 6px solid #0056a6;
   }}
   
   .district-spectrum-labels {{
     display: flex;
     justify-content: space-between;
     margin: 15px 5px 5px;
   }}
   
   .spectrum-label {{
     text-align: center;
     font-weight: bold;
     padding: 5px 12px;
     border-radius: 20px;
     font-size: 12pt;
     box-shadow: 0 1px 3px rgba(0,0,0,0.1);
   }}
   
   .spectrum-label.lowest {{
     background-color: #ffebee;
     color: #dc3545;
     border: 1px solid rgba(220,53,69,0.3);
   }}
   
   .spectrum-label.state-avg {{
     background-color: #fff8e1;
     color: #ff9800;
     border: 1px solid rgba(255,152,0,0.3);
   }}
   
   .spectrum-label.highest {{
     background-color: #e8f5e9;
     color: #28a745;
     border: 1px solid rgba(40,167,69,0.3);
   }}
   
   .district-spectrum-insight {{
     text-align: center;
     margin: 15px 0 5px;
     font-size: 14pt;
     line-height: 1.4;
   }}
   
   /* Scale Markers */
   .scale-markers {{
     position: relative;
     height: 7px;
     margin-top: -25px;
     display: flex;
     justify-content: space-between;
     padding: 0 1%;
     width: 98%;
   }}
   
   .scale-marker {{
     width: 1px;
     height: 7px;
     background-color: rgba(0,0,0,0.3);
   }}
   
   /* Table optimizations */
   table {{
     width: 100%;
     border-collapse: collapse;
     border-spacing: 0;
     margin-bottom: 20px;
   }}
   
   thead th {{
     background-color: #0056a6;
     color: white;
     font-weight: bold;
     text-align: left;
     padding: 10px 12px;
     border: 1px solid #0056a6;
   }}
   
   tbody td {{
     padding: 8px 12px;
     border: 1px solid #e0e0e0;
     vertical-align: middle;
   }}
   
   tbody tr:nth-child(even) {{
     background-color: #f9f9f9;
   }}
   
   tbody tr:hover {{
     background-color: #f0f7ff;
   }}
   
   /* Compact padding for table cells */
   .compact-table td, .compact-table th {{
     padding: 6px 10px;
   }}
   
   /* Recommendations styling */
   .recommendations-container {{
     display: grid;
     grid-template-columns: repeat(3, 1fr);
     gap: 20px;
     margin: 25px 0;
   }}
   
   .recommendation-card {{
     border: 1px solid #e0e0e0;
     border-radius: 12px;
     padding: 18px;
     background-color: white;
   }}
   
   .recommendation-card h4 {{
     margin-top: 0;
     color: #0056a6;
     padding-bottom: 8px;
     border-bottom: 1px solid #e0e0e0;
   }}
   
   .recommendation-list {{
     padding-left: 20px;
     margin: 10px 0;
   }}
   
   .recommendation-list li {{
     margin-bottom: 10px;
     line-height: 1.4;
   }}
   
   .priority-indicator {{
     display: inline-block;
     width: 10px;
     height: 10px;
     border-radius: 50%;
     margin-right: 6px;
   }}
   
   .priority-critical {{ background-color: #dc3545; }}
   .priority-high {{ background-color: #fd7e14; }}
   .priority-medium {{ background-color: #ffc107; }}
   .priority-low {{ background-color: #28a745; }}
   ```

8. Typography and space optimization:
   - Use a clean, professional sans-serif font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif
   - Set base font size to 14pt for body text to ensure readability on A2 paper
   - Scale headings proportionally: h1 (32pt), h2 (26pt), h3 (20pt), h4 (16pt)
   - Reduce line height to 1.4 to save vertical space while maintaining readability
   - Use color strategically to convey information without taking extra space
   - Minimize padding and margins where possible without compromising readability
   - Use subtle icons to convey information more efficiently than text where appropriate

**Include these exact sections, adapted for brevity:**

1. Executive Summary (Concise - Aim for ~1 page)
   - Begin with the 4 KPI cards arranged in a row using grid-4col
   - Create a modern, sleek "District Performance Spectrum" visualization
   - Include the Component Contribution table with enhanced performance capsules
   - Add a 2x2 grid SWOT analysis with concise bullet points

2. Block-wise Performance
   - Include Block Performance Comparison table with modern styling
   - Show best and worst performing blocks with brief analysis
   - Add Quick Win Opportunities section

3. Panchayat Performance
   - Create a space-efficient table showing top 10 and bottom 10 panchayats
   - Include columns for: Panchayat Name, Block Name, Score, Grade, vs State Avg
   - Add concise key observations

4. Recommendations (Comprehensive)
   - Structure as: Priority Areas, Replicating Success, Operational Improvements
   - Provide specific, actionable recommendations linked to findings
   - Include block-specific recommendations for low performers

For the District Performance Spectrum visualization, create a modern, sleek UI element like this:

```html
<div class="district-spectrum-container keep-together">
  <h3 class="district-spectrum-title">District Performance Spectrum</h3>
  <p>Visualizing where {district} ranks among all districts in Madhya Pradesh:</p>
  
  <!-- Modern, thin spectrum scale (gradient bar) -->
  <div class="district-spectrum-scale">
    <!-- Position marker based on district rank -->
    <div class="district-marker" style="left: calc({{district_position_percentage}}%)">
      <div class="marker-icon"></div>
      <div class="marker-line"></div>
      <div class="district-marker-label-below">{{district}}: {{district_score}}</div>
    </div>
    
    <!-- Scale markers -->
    <div class="scale-markers">
      <div class="scale-marker"></div>
      <div class="scale-marker"></div>
      <div class="scale-marker"></div>
      <div class="scale-marker"></div>
      <div class="scale-marker"></div>
      <div class="scale-marker"></div>
      <div class="scale-marker"></div>
      <div class="scale-marker"></div>
      <div class="scale-marker"></div>
    </div>
  </div>
  
  <div class="district-spectrum-labels">
    <div class="spectrum-label lowest">{{lowest_district_name}}: {{lowest_district_score}}</div>
    <div class="spectrum-label state-avg">State Avg: {{state_average}}</div>
    <div class="spectrum-label highest">{{highest_district_name}}: {{highest_district_score}}</div>
  </div>
  
  <p class="district-spectrum-insight">
    {district} ranks <strong>{{district_rank}}</strong> out of <strong>{{total_districts}}</strong> districts, 
    performing <strong class="{{above_or_below_class}}">{{difference_text}}</strong> the state average.
  </p>
</div>

<style>
/* Add this new style for below-positioned label */
.district-marker-label-below {{
  position: absolute;
  top: 30px; /* Position below the marker instead of above */
  left: 50%;
  transform: translateX(-50%);
  background-color: #0056a6;
  color: white;
  padding: 6px 12px;
  border-radius: 6px;
  white-space: nowrap;
  font-weight: bold;
  font-size: 13pt;
  box-shadow: 0 2px 8px rgba(0,0,0,0.2);
}}

.district-marker-label-below::before {{
  content: "";
  position: absolute;
  top: -6px; /* Arrow points upward */
  left: 50%;
  transform: translateX(-50%);
  width: 0;
  height: 0;
  border-left: 6px solid transparent;
  border-right: 6px solid transparent;
  border-bottom: 6px solid #0056a6; /* Arrow points upward */
}}
</style>
```

For the performance capsules in tables, use this modern styling:

```html
<span class="performance-capsule capsule-high"><i>★</i> High</span>
<span class="performance-capsule capsule-above"><i>↗</i> Above Avg</span>
<span class="performance-capsule capsule-average"><i>→</i> Average</span>
<span class="performance-capsule capsule-below"><i>↘</i> Below Avg</span>
<span class="performance-capsule capsule-critical"><i>⚠</i> Critical</span>
```

For the SWOT analysis, ensure you include all four quadrants in a space-efficient 2x2 grid:

```html
<div class="swot-container keep-together">
  <div class="swot-box swot-strengths">
    <h4>Strengths</h4>
    <ul>
      <!-- 3-4 concise bullet points -->
      <li>Labor Engagement score of {{labor_score}} ({{labor_percent}}% above state average)</li>
      <!-- More strengths... -->
    </ul>
  </div>
  
  <div class="swot-box swot-weaknesses">
    <h4>Weaknesses</h4>
    <ul>
      <!-- 3-4 concise bullet points -->
      <li>NMMS Usage at only {{nmms_percent}}% ({{nmms_gap}}% below target)</li>
      <!-- More weaknesses... -->
    </ul>
  </div>
  
  <div class="swot-box swot-opportunities">
    <h4>Opportunities</h4>
    <ul>
      <!-- 3-4 concise bullet points -->
      <li>Potential to improve women mate engagement from {{current_women_mate}}% to {{target_women_mate}}%</li>
      <!-- More opportunities... -->
    </ul>
  </div>
  
  <div class="swot-box swot-threats">
    <h4>Threats</h4>
    <ul>
      <!-- 3-4 concise bullet points -->
      <li>Risk of declining person-days if current trend continues</li>
      <!-- More threats... -->
    </ul>
  </div>
</div>
```

For the component performance table with enhanced capsules:

```html
<div class="card keep-together">
  <h3>Component Performance</h3>
  <table class="compact-table">
    <thead>
      <tr>
        <th>Component</th>
        <th>Score</th>
        <th>Top District</th>
        <th>Ranking</th>
        <th>Performance</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>Labor Engagement</td>
        <td>7.8 / 10</td>
        <td>3 / 52</td>
        <td>DISTRICT_NAME (9.5)</td>
        <td><span class="performance-capsule capsule-high"><i>★</i> High</span></td>
      </tr>
      <!-- More rows... -->
    </tbody>
  </table>
</div>
```
important: Top District is the topmost district in that group makesure to follow this structure for table
For the footer, use only this exact content:
```html
<div class="copyright" style="text-align: center; margin-top: 30px; font-size: 12pt; color: #555;">
    <p>&copy; Prepared By: Anshuman Raj (CEO ZP SIDHI)</p>
    <p>Data as of {date} | Report Generated on {current_time}</p>
</div>
```

CRITICAL REQUIREMENTS:
1. The entire report MUST FIT WITHIN 2-3 PAGES when printed on A2 paper. This is non-negotiable.
2. Use minimal page breaks - only when absolutely necessary between major sections.
3. Create a modern, sleek "District Performance Spectrum" visualization with a thin gradient bar.
4. Use enhanced performance capsules with icons in tables.
5. Optimize layout to minimize empty spaces throughout the report.
6. Include all four SWOT quadrants in a space-efficient design.
7. Focus on the most important insights and actionable recommendations.
8. Include top 10 and bottom 10 panchayats across the entire district with their respective block names.

Important: Create complete, valid HTML with all CSS inline to ensure consistent rendering across systems. The document will be converted to PDF using Playwright, so ensure all styling is PDF-compatible without any JavaScript. Focus on summarizing and hitting the key points to fit the 2-3 page A2 target. Use the provided data to validate points concisely.
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