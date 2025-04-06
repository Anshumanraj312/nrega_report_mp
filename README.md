# NREGA Report Generator - Madhya Pradesh

This repository contains scripts to generate comprehensive reports for NREGA (National Rural Employment Guarantee Act) data for districts in Madhya Pradesh, India.

## Setup

1. Clone this repository:
   ```
   git clone https://github.com/Anshumanraj312/nrega_report_mp.git
   cd nrega_report_mp
   ```

2. Create a virtual environment and install dependencies:
   ```
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Configure environment variables:
   ```
   cp .env.example .env
   ```
   Then edit the `.env` file with your Anthropic API key. The scripts use the Claude 3.7 Sonnet model.

## Usage

To generate a comprehensive report for a district, run:

```
python generate_comprehensive_report.py YYYY-MM-DD DISTRICT_NAME
```

For example:
```
python generate_comprehensive_report.py 2025-03-19 SIDHI
```

**Note**: District names must be in UPPERCASE as shown in the supported districts list below.

## Supported Districts

The following districts in Madhya Pradesh are supported:

1. AGAR-MALWA
2. ALIRAJPUR
3. ANUPPUR
4. ASHOK NAGAR
5. BALAGHAT
6. BARWANI
7. BETUL
8. BHIND
9. BHOPAL
10. BURHANPUR
11. CHHATARPUR
12. CHHINDWARA
13. DAMOH
14. DATIA
15. DEWAS
16. DHAR
17. DINDORI
18. GUNA
19. GWALIOR
20. HARDA
21. INDORE
22. JABALPUR
23. JHABUA
24. KATNI
25. KHANDWA
26. KHARGONE
27. MANDLA
28. MANDSAUR
29. MORENA
30. NARMADAPURAM
31. NARSINGHPUR
32. NEEMUCH
33. NIWARI
34. PANNA
35. RAISEN
36. RAJGARH
37. RATLAM
38. REWA
39. SAGAR
40. SATNA
41. SEHORE
42. SEONI
43. SHAHDOL
44. SHAJAPUR
45. SHEOPUR
46. SHIVPURI
47. SIDHI
48. SINGRAULI
49. TIKAMGARH
50. UJJAIN
51. UMARIA
52. VIDISHA

## Report Components

The comprehensive report includes analysis of:

- Area officer inspections
- Average person-days per household
- Category-wise employment
- Forest Rights Act beneficiaries
- Geotag pending works
- Labor engagement metrics
- Labour-material ratio
- National Mobile Monitoring System usage
- Women mate engagement
- Work management statistics
- Zero muster roll analysis

## Requirements

See `requirements.txt` for a complete list of dependencies.

## License and Warning ⚠️

**WARNING**: This project is proprietary and for authorized use only. No use is allowed without written permission of the Author. Strict legal actions will be started if anyone uses this without author's permission.
