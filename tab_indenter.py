import sys

def main():
    target_file = 'app.py'
    try:
        with open(target_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        new_lines = []
        in_tab_block = False
        
        tab_replacements = {
            'with tab_control:': 'if "🗄️ Data Control Room" in tab_map:\n    with tab_map["🗄️ Data Control Room"]:',
            'with tab_exec:': 'if "📊 Executive Summary" in tab_map:\n    with tab_map["📊 Executive Summary"]:',
            'with tab_financials:': 'if "🏦 Financial Deep-Dive" in tab_map:\n    with tab_map["🏦 Financial Deep-Dive"]:',
            'with tab_crm:': 'if "🕵️ CRM Intelligence" in tab_map:\n    with tab_map["🕵️ CRM Intelligence"]:',
            'with tab_campaigns:': 'if "📈 Campaigns" in tab_map:\n    with tab_map["📈 Campaigns"]:'
        }

        for line in lines:
            stripped = line.lstrip()
            indent_size = len(line) - len(stripped)
            is_empty = (stripped == '' or stripped == '\n')

            # Are we starting a block?
            matched = False
            if indent_size == 0 and not is_empty:
                for target, replacement in tab_replacements.items():
                    if stripped.startswith(target):
                        new_lines.append(replacement + '\n')
                        in_tab_block = True
                        matched = True
                        break
            
            if matched:
                continue
                
            if in_tab_block:
                if indent_size == 0 and not is_empty:
                    # We broke out of the tab scope natively
                    in_tab_block = False
                    new_lines.append(line)
                else:
                    # Add 4 extra spaces to all indented lines under the `with`
                    if not is_empty:
                        new_lines.append('    ' + line)
                    else:
                        new_lines.append(line)
            else:
                new_lines.append(line)

        with open(target_file, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
            
        print("Tab logic successfully updated and indented.")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
