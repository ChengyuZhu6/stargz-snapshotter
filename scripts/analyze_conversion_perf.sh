#!/bin/bash

# 分析 stargz 层转换性能数据的脚本
# 使用方法: ./analyze_conversion_perf.sh <log_file>

set -euo pipefail

LOGFILE="${1:-}"
if [[ -z "$LOGFILE" ]]; then
    echo "使用方法: $0 <log_file>"
    echo "例如: $0 /var/log/containerd.log"
    exit 1
fi

if [[ ! -f "$LOGFILE" ]]; then
    echo "错误: 日志文件不存在: $LOGFILE"
    exit 1
fi

# 检查是否安装了 jq
if ! command -v jq &> /dev/null; then
    echo "错误: 需要安装 jq 命令"
    echo "安装命令: apt-get install jq 或 yum install jq"
    exit 1
fi

# 临时文件
TMPFILE=$(mktemp)
trap "rm -f $TMPFILE" EXIT

echo "=== Stargz 层转换性能分析 ==="
echo "日志文件: $LOGFILE"
echo "分析时间: $(date)"
echo

# 提取性能数据
echo "正在提取性能数据..."
grep "PERF_DATA:" "$LOGFILE" | sed 's/.*PERF_DATA: //' > "$TMPFILE"

if [[ ! -s "$TMPFILE" ]]; then
    echo "错误: 在日志文件中未找到性能数据"
    echo "确保日志包含 'PERF_DATA:' 标记的JSON数据"
    exit 1
fi

TOTAL_CONVERSIONS=$(wc -l < "$TMPFILE")
echo "总转换次数: $TOTAL_CONVERSIONS"
echo

# 成功/失败统计
SUCCESSFUL=$(jq -r 'select(.success == true) | .digest' "$TMPFILE" | wc -l)
FAILED=$((TOTAL_CONVERSIONS - SUCCESSFUL))
SUCCESS_RATE=$(echo "scale=2; $SUCCESSFUL * 100 / $TOTAL_CONVERSIONS" | bc -l)

echo "=== 转换成功率 ==="
echo "成功: $SUCCESSFUL"
echo "失败: $FAILED"
echo "成功率: ${SUCCESS_RATE}%"
echo

# 总体耗时统计
echo "=== 总体耗时统计 (毫秒) ==="
jq -r 'select(.success == true) | .total_duration_ms' "$TMPFILE" | sort -n > "${TMPFILE}.total_times"
if [[ -s "${TMPFILE}.total_times" ]]; then
    MIN_TOTAL=$(head -1 "${TMPFILE}.total_times")
    MAX_TOTAL=$(tail -1 "${TMPFILE}.total_times")
    AVG_TOTAL=$(awk '{sum += $1} END {print sum/NR}' "${TMPFILE}.total_times")
    MEDIAN_TOTAL=$(awk '{a[NR]=$1} END {print (NR%2==1) ? a[(NR+1)/2] : (a[NR/2] + a[NR/2+1])/2}' "${TMPFILE}.total_times")
    
    printf "最小值: %.2f ms\n" "$MIN_TOTAL"
    printf "最大值: %.2f ms\n" "$MAX_TOTAL"
    printf "平均值: %.2f ms\n" "$AVG_TOTAL"
    printf "中位数: %.2f ms\n" "$MEDIAN_TOTAL"
fi
echo

# 各阶段耗时分析
echo "=== 各阶段平均耗时 (毫秒) ==="
for stage in layer_type_check get_content_info create_reader build_estargz prepare_writer data_copy update_commit build_descriptor; do
    avg=$(jq -r "select(.success == true) | .stages.${stage} // 0" "$TMPFILE" | awk '{sum += $1; count++} END {if(count > 0) print sum/count; else print 0}')
    printf "%-20s: %8.2f ms\n" "$stage" "$avg"
done
echo

# 各阶段总耗时分析
echo "=== 各阶段总耗时 (毫秒) ==="
for stage in layer_type_check get_content_info create_reader build_estargz prepare_writer data_copy update_commit build_descriptor; do
    total=$(jq -r "select(.success == true) | .stages.${stage} // 0" "$TMPFILE" | awk '{sum += $1} END {print sum}')
    printf "%-20s: %8.2f ms\n" "$stage" "$total"
done
echo

# 阶段耗时占比分析
echo "=== 各阶段耗时占比 ==="
TOTAL_ALL_STAGES=$(jq -r 'select(.success == true) | .total_duration_ms' "$TMPFILE" | awk '{sum += $1} END {print sum}')
for stage in layer_type_check get_content_info create_reader build_estargz prepare_writer data_copy update_commit build_descriptor; do
    total=$(jq -r "select(.success == true) | .stages.${stage} // 0" "$TMPFILE" | awk '{sum += $1} END {print sum}')
    if (( $(echo "$TOTAL_ALL_STAGES > 0" | bc -l) )); then
        percentage=$(echo "scale=2; $total * 100 / $TOTAL_ALL_STAGES" | bc -l)
        printf "%-20s: %8.2f ms (%5.1f%%)\n" "$stage" "$total" "$percentage"
    else
        printf "%-20s: %8.2f ms\n" "$stage" "$total"
    fi
done
echo

echo "=== 压缩比统计 ==="
jq -r 'select(.success == true and .compression_ratio != null) | .compression_ratio' "$TMPFILE" | sort -n > "${TMPFILE}.compression_ratios"
if [[ -s "${TMPFILE}.compression_ratios" ]]; then
    MIN_RATIO=$(head -1 "${TMPFILE}.compression_ratios")
    MAX_RATIO=$(tail -1 "${TMPFILE}.compression_ratios")
    AVG_RATIO=$(awk '{sum += $1} END {print sum/NR}' "${TMPFILE}.compression_ratios")
    MEDIAN_RATIO=$(awk '{a[NR]=$1} END {print (NR%2==1) ? a[(NR+1)/2] : (a[NR/2] + a[NR/2+1])/2}' "${TMPFILE}.compression_ratios")
    
    printf "最小压缩比: %.2f%%\n" "$MIN_RATIO"
    printf "最大压缩比: %.2f%%\n" "$MAX_RATIO"
    printf "平均压缩比: %.2f%%\n" "$AVG_RATIO"
    printf "中位数压缩比: %.2f%%\n" "$MEDIAN_RATIO"
fi
echo

# 大小统计
echo "=== 大小统计 ==="
TOTAL_ORIGINAL=$(jq -r 'select(.success == true) | .original_size' "$TMPFILE" | awk '{sum += $1} END {print sum}')
TOTAL_COMPRESSED=$(jq -r 'select(.success == true) | .compressed_size' "$TMPFILE" | awk '{sum += $1} END {print sum}')
OVERALL_RATIO=$(echo "scale=2; $TOTAL_COMPRESSED * 100 / $TOTAL_ORIGINAL" | bc -l)

echo "总原始大小: $(numfmt --to=iec-i --suffix=B $TOTAL_ORIGINAL)"
echo "总压缩大小: $(numfmt --to=iec-i --suffix=B $TOTAL_COMPRESSED)"
echo "总体压缩比: ${OVERALL_RATIO}%"
echo

# 最耗时的转换
echo "=== 最耗时的 5 次转换 ==="
jq -r 'select(.success == true) | [.digest[0:12], .total_duration_ms, .original_size, .compressed_size] | @tsv' "$TMPFILE" | \
    sort -k2 -nr | head -5 | \
    awk 'BEGIN {printf "%-15s %10s %12s %12s\n", "Digest", "Time(ms)", "Original", "Compressed"} 
         {printf "%-15s %10.2f %12s %12s\n", $1, $2, $3, $4}'
echo

echo "分析完成！"
echo "原始数据保存在: $TMPFILE"
echo "可以使用 jq 进行进一步分析，例如:"
echo "  jq '.stages | keys[]' $TMPFILE | sort | uniq"
echo "  jq 'select(.total_duration_ms > 1000)' $TMPFILE" 