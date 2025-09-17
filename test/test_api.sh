#!/bin/bash

# Configuration
API_URL="http://localhost:5000/v1/chat"
TENANT_ID="company-a"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Create timestamp for files
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_DIR="api_test_results_${TIMESTAMP}"
DETAIL_LOG="${LOG_DIR}/detailed_results.log"
SUMMARY_LOG="${LOG_DIR}/summary_report.txt"
CSV_LOG="${LOG_DIR}/results.csv"
JSON_LOG="${LOG_DIR}/responses.json"
ERROR_LOG="${LOG_DIR}/errors.log"

# Counters
TOTAL_TESTS=0
SUCCESS_COUNT=0
FAILED_COUNT=0
START_TIME=$(date +%s)

# Create log directory
mkdir -p "$LOG_DIR"

# Initialize CSV file
echo "timestamp,question_number,category,question,status,response_time,response_size,http_code" > "$CSV_LOG"

# Initialize JSON array
echo "[" > "$JSON_LOG"
FIRST_JSON=true

# Function to log messages
log_message() {
    local message="$1"
    echo "$message" | tee -a "$DETAIL_LOG"
}

# Function to send request and save response
send_request() {
    local question="$1"
    local category="$2"
    local question_num="$3"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    # Display progress
    echo -e "${BLUE}[${question_num}/100]${NC} ${BLUE}[$category]${NC} Testing: $question"
    
    # Log to detailed file
    echo "===========================================" >> "$DETAIL_LOG"
    echo "[Test #${question_num}]" >> "$DETAIL_LOG"
    echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')" >> "$DETAIL_LOG"
    echo "Category: $category" >> "$DETAIL_LOG"
    echo "Question: $question" >> "$DETAIL_LOG"
    echo "-------------------------------------------" >> "$DETAIL_LOG"
    
    # Prepare JSON payload
    json_payload=$(cat <<EOF
{
    "question": "$question",
    "tenant_id": "$TENANT_ID"
}
EOF
    )
    
    # Save request
    echo "Request:" >> "$DETAIL_LOG"
    echo "$json_payload" >> "$DETAIL_LOG"
    echo "" >> "$DETAIL_LOG"
    
    # Send request and capture response with timing
    local start_req=$(date +%s%N)
    
    response=$(curl -s -w "\n%{http_code}\n%{size_download}" -X POST "$API_URL" \
        -H "Content-Type: application/json" \
        -d "$json_payload" 2>> "$ERROR_LOG")
    
    local end_req=$(date +%s%N)
    local response_time=$((($end_req - $start_req) / 1000000)) # Convert to milliseconds
    
    # Parse response
    http_code=$(echo "$response" | tail -n 2 | head -n 1)
    response_size=$(echo "$response" | tail -n 1)
    response_body=$(echo "$response" | head -n -2)
    
    # Determine status
    if [ "$http_code" = "200" ]; then
        status="SUCCESS"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        echo -e "${GREEN}✓ Success${NC} (${response_time}ms)"
    else
        status="FAILED"
        FAILED_COUNT=$((FAILED_COUNT + 1))
        echo -e "${RED}✗ Failed${NC} (HTTP $http_code)"
        
        # Log error
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Question #${question_num}: HTTP $http_code - $question" >> "$ERROR_LOG"
    fi
    
    # Save response to detailed log
    echo "Response (HTTP $http_code):" >> "$DETAIL_LOG"
    echo "$response_body" >> "$DETAIL_LOG"
    echo "Response Time: ${response_time}ms" >> "$DETAIL_LOG"
    echo "Response Size: ${response_size} bytes" >> "$DETAIL_LOG"
    echo "" >> "$DETAIL_LOG"
    
    # Save to CSV
    echo "$(date '+%Y-%m-%d %H:%M:%S'),${question_num},${category},\"${question}\",${status},${response_time},${response_size},${http_code}" >> "$CSV_LOG"
    
    # Save to JSON
    if [ "$FIRST_JSON" = true ]; then
        FIRST_JSON=false
    else
        echo "," >> "$JSON_LOG"
    fi
    
    cat >> "$JSON_LOG" <<EOF
    {
        "test_number": ${question_num},
        "timestamp": "$(date '+%Y-%m-%d %H:%M:%S')",
        "category": "${category}",
        "question": "${question}",
        "status": "${status}",
        "http_code": ${http_code:-0},
        "response_time_ms": ${response_time},
        "response_size_bytes": ${response_size:-0},
        "request": ${json_payload},
        "response": ${response_body:-null}
    }
EOF
    
    # Small delay between requests
    sleep 0.3
}

# Main execution
clear
echo "=========================================="
echo "   HVAC API Test Suite - 100 Questions   "
echo "=========================================="
echo "API URL: $API_URL"
echo "Tenant ID: $TENANT_ID"
echo "Log Directory: $LOG_DIR"
echo "Start Time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
echo ""

# Initialize summary report
cat > "$SUMMARY_LOG" <<EOF
=====================================
HVAC API Test Suite - Summary Report
=====================================
Test Date: $(date '+%Y-%m-%d %H:%M:%S')
API URL: $API_URL
Tenant ID: $TENANT_ID
=====================================

EOF

# หมวด 1: ข้อมูลรายได้และยอดขาย (1-25)
echo -e "\n${YELLOW}=== หมวด 1: ข้อมูลรายได้และยอดขาย ===${NC}\n"
echo "=== หมวด 1: ข้อมูลรายได้และยอดขาย ===" >> "$SUMMARY_LOG"

send_request "รายได้รวมทั้งหมดเท่าไหร่" "รายได้" 1
send_request "รายได้ปี 2024 เป็นเท่าไหร่" "รายได้" 2
send_request "เปรียบเทียบรายได้ปี 2023 กับ 2024" "รายได้" 3
send_request "ยอดขาย overhaul ทั้งหมดเท่าไหร่" "รายได้" 4
send_request "ยอดขาย service ปี 2024" "รายได้" 5
send_request "ยอดขาย parts หรืออะไหล่มีเท่าไหร่" "รายได้" 6
send_request "ยอดขาย replacement หรือเปลี่ยนอุปกรณ์" "รายได้" 7
send_request "รายได้แต่ละปีเป็นอย่างไร" "รายได้" 8
send_request "ยอดขายแยกตามประเภทงาน" "รายได้" 9
send_request "รายได้เฉลี่ยต่อปีเท่าไหร่" "รายได้" 10
send_request "ปีไหนมีรายได้สูงสุด" "รายได้" 11
send_request "ปีไหนมีรายได้ต่ำสุด" "รายได้" 12
send_request "งานที่มีมูลค่าสูงสุดคืองานไหน" "รายได้" 13
send_request "งานที่มีมูลค่าต่ำสุด" "รายได้" 14
send_request "รายได้จาก overhaul ปี 2024" "รายได้" 15
send_request "รายได้จาก service ปี 2023 เท่าไหร่" "รายได้" 16
send_request "มีงานทั้งหมดกี่งาน" "รายได้" 17
send_request "มีงานปี 2024 กี่งาน" "รายได้" 18
send_request "รายได้เฉลี่ยต่องาน" "รายได้" 19
send_request "งานที่มีรายได้มากกว่า 1 ล้านบาท" "รายได้" 20
send_request "งานที่มีรายได้น้อยกว่า 50000 บาท" "รายได้" 21
send_request "การเติบโตรายได้จาก 2023 เป็น 2024" "รายได้" 22
send_request "สัดส่วนรายได้แต่ละประเภทงาน" "รายได้" 23
send_request "งานที่มีรายได้สูงสุดแต่ละปี" "รายได้" 24
send_request "มูลค่ารวมของสินค้าคงคลัง" "รายได้" 25

# หมวด 2: ข้อมูลลูกค้า (26-50)
echo -e "\n${YELLOW}=== หมวด 2: ข้อมูลลูกค้า ===${NC}\n"
echo -e "\n=== หมวด 2: ข้อมูลลูกค้า ===" >> "$SUMMARY_LOG"

send_request "มีลูกค้าทั้งหมดกี่ราย" "ลูกค้า" 26
send_request "Top 10 ลูกค้าที่ใช้บริการมากที่สุด" "ลูกค้า" 27
send_request "ลูกค้าที่มียอดการใช้บริการสูงสุด" "ลูกค้า" 28
send_request "ประวัติการใช้บริการของ Stanley" "ลูกค้า" 29
send_request "ประวัติการใช้บริการของ Clarion" "ลูกค้า" 30
send_request "ลูกค้าใหม่ปี 2024 มีใครบ้าง" "ลูกค้า" 31
send_request "ลูกค้าที่ใช้บริการบ่อยที่สุด" "ลูกค้า" 32
send_request "ลูกค้าที่ใช้บริการ overhaul" "ลูกค้า" 33
send_request "ลูกค้าภาครัฐมีใครบ้าง" "ลูกค้า" 34
send_request "ลูกค้าเอกชนที่ใหญ่ที่สุด" "ลูกค้า" 35
send_request "ข้อมูลลูกค้า Toyota" "ลูกค้า" 36
send_request "ข้อมูลลูกค้า Denso" "ลูกค้า" 37
send_request "ลูกค้าที่เคยใช้บริการแต่ไม่ได้ใช้ปี 2024" "ลูกค้า" 38
send_request "ลูกค้าที่ใช้บริการต่อเนื่องทุกปี" "ลูกค้า" 39
send_request "จำนวนลูกค้าที่ใช้บริการแต่ละปี" "ลูกค้า" 40
send_request "ลูกค้าที่เกี่ยวข้องกับ Hitachi" "ลูกค้า" 41
send_request "ลูกค้าโรงพยาบาลมีใครบ้าง" "ลูกค้า" 42
send_request "ลูกค้าที่จ่ายมากกว่า 500000 บาท" "ลูกค้า" 43
send_request "รายได้เฉลี่ยต่อลูกค้า" "ลูกค้า" 44
send_request "ลูกค้าที่ใช้บริการเฉพาะซื้ออะไหล่" "ลูกค้า" 45
send_request "ข้อมูลลูกค้า Master Glove" "ลูกค้า" 46
send_request "ข้อมูลลูกค้า IRPC" "ลูกค้า" 47
send_request "ลูกค้าต่างชาติมีใครบ้าง" "ลูกค้า" 48
send_request "ลูกค้าที่มีงานเกี่ยวกับ Chiller" "ลูกค้า" 49
send_request "เปรียบเทียบรายได้จากลูกค้าใหม่กับลูกค้าเก่า" "ลูกค้า" 50

# หมวด 3: ข้อมูลอะไหล่และสินค้าคงคลัง (51-70)
echo -e "\n${YELLOW}=== หมวด 3: ข้อมูลอะไหล่และสินค้าคงคลัง ===${NC}\n"
echo -e "\n=== หมวด 3: ข้อมูลอะไหล่และสินค้าคงคลัง ===" >> "$SUMMARY_LOG"

send_request "มีอะไหล่ทั้งหมดกี่รายการ" "อะไหล่" 51
send_request "อะไหล่ที่มีสต็อกคงเหลือ" "อะไหล่" 52
send_request "อะไหล่ที่หมดสต็อก" "อะไหล่" 53
send_request "อะไหล่ที่มีราคาแพงที่สุด" "อะไหล่" 54
send_request "อะไหล่ที่มีราคาถูกที่สุด" "อะไหล่" 55
send_request "อะไหล่ที่มีสต็อกน้อยกว่า 5 ชิ้น" "อะไหล่" 56
send_request "อะไหล่ในคลัง A" "อะไหล่" 57
send_request "อะไหล่ที่มีรหัส EKAC" "อะไหล่" 58
send_request "อะไหล่ที่มีมูลค่ารวมสูงสุด" "อะไหล่" 59
send_request "ราคาเฉลี่ยของอะไหล่" "อะไหล่" 60
send_request "จำนวนอะไหล่ทั้งหมดที่มีในสต็อก" "อะไหล่" 61
send_request "อะไหล่สำหรับ compressor" "อะไหล่" 62
send_request "อะไหล่ filter มีอะไรบ้าง" "อะไหล่" 63
send_request "อะไหล่ที่ต้องสั่งเติม" "อะไหล่" 64
send_request "มูลค่าสต็อกรวมทั้งหมด" "อะไหล่" 65
send_request "อะไหล่ที่ไม่มีการตั้งราคา" "อะไหล่" 66
send_request "คลังไหนมีอะไหล่มากที่สุด" "อะไหล่" 67
send_request "ข้อมูลอะไหล่รหัส EKAC460" "อะไหล่" 68
send_request "อะไหล่ที่ขายเป็น SET" "อะไหล่" 69
send_request "สินค้าที่เพิ่งรับเข้าล่าสุด" "อะไหล่" 70

# หมวด 4: ข้อมูลงานบริการและทีมงาน (71-90)
echo -e "\n${YELLOW}=== หมวด 4: ข้อมูลงานบริการและทีมงาน ===${NC}\n"
echo -e "\n=== หมวด 4: ข้อมูลงานบริการและทีมงาน ===" >> "$SUMMARY_LOG"

send_request "มีงานทั้งหมดกี่งานในระบบ" "งานบริการ" 71
send_request "งานเดือนกันยายน 2024" "งานบริการ" 72
send_request "งานบำรุงรักษา PM ทั้งหมด" "งานบริการ" 73
send_request "งาน overhaul ที่ทำ" "งานบริการ" 74
send_request "งานเปลี่ยนอุปกรณ์" "งานบริการ" 75
send_request "งานที่ทำสำเร็จ" "งานบริการ" 76
send_request "งานที่ไม่สำเร็จ" "งานบริการ" 77
send_request "งานของทีม A" "งานบริการ" 78
send_request "งานวันนี้มีอะไรบ้าง" "งานบริการ" 79
send_request "งานสัปดาห์นี้" "งานบริการ" 80
send_request "อัตราความสำเร็จของงาน" "งานบริการ" 81
send_request "งานที่เสร็จตรงเวลา ไม่เกิน 2 วัน" "งานบริการ" 82
send_request "งานที่ทำเกินเวลา" "งานบริการ" 83
send_request "งานทั้งหมดของลูกค้า Stanley" "งานบริการ" 84
send_request "งาน start up เริ่มต้นระบบ" "งานบริการ" 85
send_request "งานสนับสนุนทั่วไป" "งานบริการ" 86
send_request "งาน CPA" "งานบริการ" 87
send_request "สถิติงานของแต่ละทีม" "งานบริการ" 88
send_request "ระยะเวลาการทำงานแต่ละงาน" "งานบริการ" 89
send_request "งาน 10 งานล่าสุด" "งานบริการ" 90

# หมวด 5: คำถามเชิงวิเคราะห์และรายงาน (91-100)
echo -e "\n${YELLOW}=== หมวด 5: คำถามเชิงวิเคราะห์และรายงาน ===${NC}\n"
echo -e "\n=== หมวด 5: คำถามเชิงวิเคราะห์และรายงาน ===" >> "$SUMMARY_LOG"

send_request "สรุปผลประกอบการแต่ละปี" "วิเคราะห์" 91
send_request "เทรนด์การเติบโตของธุรกิจ" "วิเคราะห์" 92
send_request "ประเภทงานที่ลูกค้านิยมใช้บริการมากที่สุด" "วิเคราะห์" 93
send_request "ลูกค้าที่มีศักยภาพสูง ใช้บริการบ่อยและจ่ายเยอะ" "วิเคราะห์" 94
send_request "การกระจายรายได้ตามขนาดงาน" "วิเคราะห์" 95
send_request "ประสิทธิภาพการทำงานของแต่ละทีม" "วิเคราะห์" 96
send_request "แนวโน้มยอดขายรายเดือนปี 2024" "วิเคราะห์" 97
send_request "ผลตอบแทนจากการลงทุนในแต่ละประเภทบริการ" "วิเคราะห์" 98
send_request "คาดการณ์รายได้ปี 2025 จากเทรนด์" "วิเคราะห์" 99
send_request "รายงานสถานะธุรกิจโดยรวม" "วิเคราะห์" 100

# Close JSON array
echo -e "\n]" >> "$JSON_LOG"

# Calculate statistics
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
SUCCESS_RATE=$(awk "BEGIN {printf \"%.2f\", ($SUCCESS_COUNT/$TOTAL_TESTS)*100}")

# Calculate average response time from CSV
AVG_RESPONSE_TIME=$(awk -F',' 'NR>1 {sum+=$6; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}' "$CSV_LOG")

# Generate final summary
cat >> "$SUMMARY_LOG" <<EOF

=====================================
TEST RESULTS SUMMARY
=====================================
Total Tests: $TOTAL_TESTS
Successful: $SUCCESS_COUNT
Failed: $FAILED_COUNT
Success Rate: ${SUCCESS_RATE}%
Average Response Time: ${AVG_RESPONSE_TIME}ms
Total Duration: ${DURATION} seconds
=====================================

CATEGORY BREAKDOWN:
-------------------
EOF

# Add category statistics
for category in "รายได้" "ลูกค้า" "อะไหล่" "งานบริการ" "วิเคราะห์"; do
    cat_success=$(grep ",$category," "$CSV_LOG" | grep ",SUCCESS," | wc -l)
    cat_failed=$(grep ",$category," "$CSV_LOG" | grep ",FAILED," | wc -l)
    cat_total=$((cat_success + cat_failed))
    if [ $cat_total -gt 0 ]; then
        cat_rate=$(awk "BEGIN {printf \"%.2f\", ($cat_success/$cat_total)*100}")
        echo "$category: $cat_success/$cat_total (${cat_rate}%)" >> "$SUMMARY_LOG"
    fi
done

echo "" >> "$SUMMARY_LOG"
echo "=====================================
FILES GENERATED:
=====================================
1. Detailed Results: $DETAIL_LOG
2. Summary Report: $SUMMARY_LOG  
3. CSV Data: $CSV_LOG
4. JSON Responses: $JSON_LOG
5. SQL Queries: $SQL_LOG
6. Error Log: $ERROR_LOG
=====================================" >> "$SUMMARY_LOG"

# Display final summary
echo ""
echo "=========================================="
echo -e "${GREEN}TEST COMPLETED!${NC}"
echo "=========================================="
echo "Total Tests: $TOTAL_TESTS"
echo -e "Successful: ${GREEN}$SUCCESS_COUNT${NC}"
echo -e "Failed: ${RED}$FAILED_COUNT${NC}"
echo -e "Success Rate: ${YELLOW}${SUCCESS_RATE}%${NC}"
echo -e "SQL Queries Generated: ${BLUE}$SQL_COUNT${NC} (${SQL_PERCENTAGE}%)"
echo "Average Response Time: ${AVG_RESPONSE_TIME}ms"
echo "Total Duration: ${DURATION} seconds"
echo "=========================================="
echo ""
echo "📁 Results saved in: ${BLUE}$LOG_DIR${NC}"
echo ""
echo "Files generated:"
echo "  • detailed_results.log - Full test details"
echo "  • summary_report.txt - Test summary"
echo "  • results.csv - Data in CSV format"
echo "  • responses.json - All API responses"
echo "  • generated_queries.sql - SQL queries generated"
echo "  • errors.log - Error messages"
echo ""
echo "View summary: cat $SUMMARY_LOG"
echo "View SQL queries: cat $SQL_LOG"
echo "View CSV: cat $CSV_LOG | column -t -s,"
echo ""

# Optional: Open summary in default text editor
read -p "Do you want to view the summary report? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if command -v less &> /dev/null; then
        less "$SUMMARY_LOG"
    else
        cat "$SUMMARY_LOG"
    fi
fi