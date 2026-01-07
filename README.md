# ExcelPro（企业版）Excel 数据处理工具

ExcelPro 是一个基于 Web 的 Excel 批处理与数据整合工具，专注于解决多 Excel 文件、多工作表的批量处理需求。通过浏览器提供直观的操作界面，让用户无需编程即可完成常见的数据整合与拆分任务。

## 功能特性

### 1. 数据表拼接（Data Concatenator）
- 支持同时选择多个 Excel 文件
- 自动读取每个文件的所有 Sheet
- 检测各 Sheet 的数据结构是否一致
- 将结构一致的数据表垂直堆叠合并
- 导出为一个新的 Excel 文件

### 2. Sheet 合并（Sheet Merger）
- 支持同时选择多个 Excel 文件
- 将每个 Excel 文件的所有 Sheet 复制到一个新的工作簿中
- 自动处理重名 Sheet（添加序号后缀）
- 导出合并后的 Excel 文件

### 3. 数据关联（Data Joiner）
- 支持选择多个 Excel 文件
- 读取所有文件的所有 Sheet
- 提供关键字段选择器进行数据关联
- 支持多种连接方式（左连接、内连接、外连接）
- 自动处理列名冲突（增加后缀区分）

### 4. Sheet 拆分（Sheet Splitter）
- 支持单个 Excel 文件上传
- 将每个 Sheet 导出为一个独立的 Excel 文件
- 打包所有拆分文件为 ZIP 供下载

## 安装与运行

### 环境要求
- Python 3.10+（建议）
- Windows/macOS/Linux

## 在 PyCharm Terminal 中创建虚拟环境并运行（Windows / PowerShell）

> 适用于：Windows + PyCharm 内置 Terminal（PowerShell）
> 常见问题：系统 `python` 命令不可用时，使用 `py` 更稳定。

### ✅ 完整正确操作流程（在 PyCharm Terminal 中执行）

```powershell
# 1) 进入项目目录（路径带空格要加引号）
cd "F:\Deep Mind\Order_Management_Software\Order_Management_Software\ExcelPro"

# 2) 使用 py 创建虚拟环境（解决 python 未识别）
py -m venv venv

# 3) 设置 PowerShell 执行策略（允许运行脚本，仅对当前用户生效）
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# 4) 激活虚拟环境
venv\Scripts\Activate.ps1


### 安装依赖
```bash
pip install -r requirements.txt




# Excel Pro

Enterprise-style Excel processing tool built with Python + Streamlit:
- Processing queue (background worker)
- Batch job history (SQLite)
- Excel/CSV/PDF conversions and sheet operations

## Run
pip install -r requirements.txt
streamlit run app.py

## Notes
- All uploads and outputs are stored locally (data/ and artifacts/).
- Default worker count is 1 for stability.

## License
Apache-2.0
