"""Patch Comfyui_TTP_Toolset for ComfyUI 0.18.2 compatibility.

Original uses comfy_api.latest (requires ComfyUI >= 0.3.x).
This patches LTXVFirstLastFrameControl_TTP to use traditional format.
"""
import re
import sys
import os

filepath = os.path.join(
    os.path.dirname(__file__) if os.path.dirname(__file__) else '.',
    'LTXVFirstLastFrameControl_TTP.py'
)

# Allow path override via argument
if len(sys.argv) > 1:
    filepath = sys.argv[1]

with open(filepath, 'r') as f:
    content = f.read()

# 1. Remove comfy_api import
content = content.replace(
    'from comfy_api.latest import io',
    '# comfy_api removed for ComfyUI 0.18.2 compat'
)

# 2. Remove io.ComfyNode inheritance
content = content.replace(
    'class LTXVFirstLastFrameControl_TTP(io.ComfyNode):',
    'class LTXVFirstLastFrameControl_TTP:'
)

# 3. Remove define_schema method
content = re.sub(
    r'    @classmethod\s*\n    def define_schema\(cls\):.*?(?=\n    @classmethod\s*\n    def INPUT_TYPES)',
    '',
    content,
    flags=re.DOTALL
)

# 4. Change execute from @classmethod to instance method
content = content.replace(
    '    @classmethod\n    def execute(\n        cls,',
    '    def execute(\n        self,'
)
content = content.replace('cls._encode_image(', 'LTXVFirstLastFrameControl_TTP._encode_image(')

# 5. Remove io.NodeOutput return type hint
content = content.replace(') -> io.NodeOutput:', '):')

# 6. Remove generate = execute alias
content = content.replace('    # \u517c\u5bb9\u65e7API\n    generate = execute', '')

with open(filepath, 'w') as f:
    f.write(content)

print('=== TTP Toolset patched for ComfyUI 0.18.2 ===')
