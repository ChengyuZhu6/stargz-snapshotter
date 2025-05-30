#!/bin/bash

# 将 stargz 层转换性能数据导出为 CSV 格式
# 使用方法: ./export_conversion_csv.sh <log_file> [output.csv]

set -euo pipefail

LOGFILE="${1:-}"
CSVFILE="${2:-conversion_performance.csv}"

if [[ -z "$LOGFILE" ]]; then
    echo "使用方法: $0 <log_file> [output.csv]"
    echo "例如: $0 /var/log/containerd.log performance.csv"
    exit 1
fi

if [[ ! -f "$LOGFILE" ]]; then
    echo "错误: 日志文件不存在: $LOGFILE"
    exit 1
fi

# 检查是否安装了 jq
if ! command -v jq &> /dev/null; then
    echo "错误: 需要安装 jq 命令"
    exit 1
fi

# 临时文件
TMPFILE=$(mktemp)
trap "rm -f $TMPFILE" EXIT

echo "正在从日志文件提取性能数据..."
grep "PERF_DATA:" "$LOGFILE" | sed 's/.*PERF_DATA: //' > "$TMPFILE"

if [[ ! -s "$TMPFILE" ]]; then
    echo "错误: 在日志文件中未找到性能数据"
    exit 1
fi

echo "正在导出 CSV 文件: $CSVFILE"

# CSV 头部
cat > "$CSVFILE" << 'EOF'
timestamp,digest,success,original_size,compressed_size,compression_ratio,total_duration_ms,pure_copy_ms,layer_type_check,get_content_info,create_reader,build_estargz,prepare_writer,data_copy,update_commit,build_descriptor,error
EOF

# 数据行
jq -r '
[
  .timestamp // "",
  .digest // "",
  .success // false,
  .original_size // 0,
  .compressed_size // 0,
  .compression_ratio // 0,
  .total_duration_ms // 0,
  .pure_copy_ms // 0,
  .stages.layer_type_check // 0,
  .stages.get_content_info // 0,
  .stages.create_reader // 0,
  .stages.build_estargz // 0,
  .stages.prepare_writer // 0,
  .stages.data_copy // 0,
  .stages.update_commit // 0,
  .stages.build_descriptor // 0,
  .error // ""
] | @csv
' "$TMPFILE" >> "$CSVFILE"

echo "CSV 导出完成！"
echo "输出文件: $CSVFILE"
echo "记录数: $(tail -n +2 "$CSVFILE" | wc -l)"
echo
echo "可以使用以下命令进行进一步分析:"
echo "  # 查看最耗时的转换"
echo "  sort -t, -k7 -nr '$CSVFILE' | head -10"
echo
echo "  # 计算各阶段平均耗时"
echo "  awk -F, 'NR>1 && \$3==\"true\" {sum9+=\$9; sum10+=\$10; sum11+=\$11; sum12+=\$12; sum13+=\$13; sum14+=\$14; sum15+=\$15; sum16+=\$16; count++} END {if(count>0) printf \"layer_type_check: %.2f\\nget_content_info: %.2f\\ncreate_reader: %.2f\\nbuild_estargz: %.2f\\nprepare_writer: %.2f\\ndata_copy: %.2f\\nupdate_commit: %.2f\\nbuild_descriptor: %.2f\\n\", sum9/count, sum10/count, sum11/count, sum12/count, sum13/count, sum14/count, sum15/count, sum16/count}' '$CSVFILE'"
echo
echo "  # 查看失败的转换"
echo "  awk -F, 'NR>1 && \$3==\"false\"' '$CSVFILE'" 