#!/usr/bin/env python3
import glob
import re
import os

BATCH_DIR = "/home/dsomil/projects/bdaproject/batch/mapreduce_job"

for py_file in sorted(glob.glob(os.path.join(BATCH_DIR, "*.py"))):
    with open(py_file, "r", encoding="utf-8") as f:
        content = f.read()
    
    original = content
    # Common f-strings in these mappers:
    # f"{host}\t1" -> "{}\t1".format(host)
    # f"{...}\t{...}" -> "{}\t{}".format(..., ...)
    # Let's inspect and replace
    lines = content.splitlines(keepends=True)
    new_lines = []
    for line in lines:
        if 'f"' in line or "f'" in line:
            # Replace f"{a}\t1" -> "{}\t1".format(a)
            # Replace f"{a}\t{b}" -> "{}\t{}".format(a, b)
            # Use regex to convert f"..."
            def repl_double(m):
                s = m.group(1)
                # extract expressions inside {expr}
                exprs = re.findall(r'\{([^}]+)\}', s)
                tmpl = re.sub(r'\{[^}]+\}', '{}', s)
                if exprs:
                    return '"{tmpl}".format({args})'.format(tmpl=tmpl, args=', '.join(exprs))
                return '"{}"'.format(s)

            def repl_single(m):
                s = m.group(1)
                exprs = re.findall(r'\{([^}]+)\}', s)
                tmpl = re.sub(r'\{[^}]+\}', '{}', s)
                if exprs:
                    return '"{tmpl}".format({args})'.format(tmpl=tmpl, args=', '.join(exprs))
                return "'{}'".format(s)

            line = re.sub(r'f"([^"\n]*)"', repl_double, line)
            line = re.sub(r"f'([^'\n]*)'", repl_single, line)
        new_lines.append(line)

    new_content = "".join(new_lines)
    if new_content != original:
        with open(py_file, "w", encoding="utf-8") as f:
            f.write(new_content)
        print("Updated:", os.path.basename(py_file))
    else:
        print("Unchanged:", os.path.basename(py_file))
