# Homework 1: Data Lineage Analysis Agent

## Project Overview
I built a **data lineage analysis agent** that helps users understand how data flows through code by identifying sources, operations, and transformations.

## Task & Implementation

### ✅ Multi-turn Interaction/Tool Use
- **Task**: Code lineage analysis requiring multiple tool calls to extract sources and operations
- **Agent**: Uses `extract_sources_tool` and `extract_operations_tool` in sequence
- **Multi-turn**: Agent first analyzes code structure, then synthesizes findings into clear explanations

### ✅ Agent Scaffold Implementation
I implemented two approaches:

1. **SimpleLineageAgent** - Direct OpenAI API calls with prompt engineering
2. **Advanced Lineage Agent** - Framework-based agent with structured tool usage:
   ```python
   lineage_agent = Agent(
       model="gpt-4.1-nano",
       instructions="Expert data lineage analyst instructions...",
       tools=[extract_sources_tool, extract_operations_tool]
   )
   ```

### ✅ Test Cases & Test Prompts
Created comprehensive test suite in `test_cases.py`:
- **Basic cases**: Simple filters (Pandas/PySpark)
- **Complex cases**: Multi-step pipelines with groupby, merges, pivots
- **Expected outputs**: Sources and operations for each test case

Example test cases:
- Basic PySpark filter
- Pandas groupby + merge workflows
- Complex data transformations

### ✅ Reward Function
Implemented deterministic reward function:
```python
def simple_reward(agent_response, expected_sources, expected_operations):
    # Checks if agent response mentions expected sources/operations
    # Returns score 0-1 based on recall
```

### ✅ Evaluation Methods
Two evaluation approaches:

1. **Simple Reward Function**: Deterministic scoring based on source/operation detection
2. **LLM Judge Agent**: Uses same tools to find ground truth, then evaluates agent performance

### ✅ Testing & Results
- Tested agent on multiple test cases
- **Results**: Agent achieving 0.8-1.0 scores on basic cases
- **Performance**: Successfully identifies sources and operations, provides clear explanations

## Tools Created
Built `CodeAnalysisTools` class with methods:
- `extract_table_sources()` - Finds CSV/table sources using regex
- `extract_operations()` - Detects pandas/spark operations  
- `trace_variable_dependencies()` - Traces variable lineage

## Files Structure
```
├── homework_1.ipynb          # Main implementation notebook
├── test_cases.py             # Comprehensive test suite
├── tools.py                  # Code analysis tools
└── README.md                 # This file
```

## Key Findings

### What Worked Well
- **Tool-based approach**: Using structured tools for code analysis was more reliable than pure LLM
- **Deterministic evaluation**: Simple reward function provided clearer metrics than pure LLM judges
- **Agent framework**: Structured agent with clear instructions performed better than ad-hoc prompting

### Challenges Encountered
- **LLM Judge limitations**: Judge agents tend to do fuzzy similarity rather than systematic evaluation
- **Complex operation detection**: Some advanced pandas operations harder to detect with regex
- **Evaluation consistency**: LLM judges sometimes inconsistent compared to deterministic metrics

### Model Performance
- **gpt-4.1-nano**: Worked well for this structured task with clear tool usage
- **Evaluation**: Simple reward function more reliable than LLM judge for this specific domain

## Future Improvements
1. **Enhanced operation detection**: Better regex patterns for complex transformations
2. **Multi-model testing**: Systematic comparison across different model sizes
3. **Structured evaluation**: More deterministic evaluation metrics
4. **Tool refinement**: Better handling of edge cases in code parsing

## Usage
```python
# Run lineage analysis
result = await Runner.run(lineage_agent, f"Analyze lineage of '{target}' in: {code}")

# Evaluate with reward function  
score = simple_reward(result.final_output, expected_sources, expected_operations)
```