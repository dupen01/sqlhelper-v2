import gradio as gr

from src.utils import get_first_level_source_tables, get_mermaid_str


def handle_sql(sql: str, operation):
    if operation == "获取全部原始底表":
        return get_first_level_source_tables(sql)

    elif operation == "获取表级血缘关系":
        return get_mermaid_str(sql)

    else:
        return "Unknown operation"


demo = gr.Interface(
    fn=handle_sql,
    inputs=[
        gr.Code(language="sql", label="sql", scale=7),
        gr.Radio(["获取表级血缘关系", "获取全部原始底表"], value="获取表级血缘关系", label="Operation"),
    ],
    outputs=[
        gr.Code(label="Output", language="markdown", scale=3),
    ],
    title="简易SQL处理器",
    submit_btn="提交",
    flagging_mode="never",
)

demo.launch()
