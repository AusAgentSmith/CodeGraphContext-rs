"""
Test script for the enhanced C++ parser.
"""
import json
import sys
from pathlib import Path


def test_parser():
    """Test the C++ parser with a sample file."""
    test_cpp_code = '''

class TestClass : public BaseClass {
private:
    int privateVar;
    std::string name;

protected:
    double protectedVar;

public:
    /**
     * Constructor for TestClass
     * @param value Initial value
     */
    TestClass(int value) : privateVar(value) {}
    
    /// Gets the private variable value
    int getValue() const {
        return privateVar;
    }
    
    /**
     * Complex method with parameters
     */
    virtual void complexMethod(int x, const std::string& str, double y = 3.14) override {
        if (x > 0) {
            for (int i = 0; i < x; i++) {
                processData(i);
            }
        }
    }
};

namespace MyNamespace {
    enum Color {
        RED,
        GREEN,
        BLUE
    };
    
    /**
     * Standalone function
     */
    static inline int calculate(int a, int b) {
        return a + b;
    }
}

// Global variable
int globalCounter = 0;

#include <iostream>
#include <string>
#include "custom_header.h"
'''
    test_file = Path("test_sample.cpp")
    test_file.write_text(test_cpp_code)
    
    print("=" * 60)
    print("Testing Enhanced C++ Parser")
    print("=" * 60)
    
    try:
        from tree_sitter import Language, Parser
        import tree_sitter_cpp as tscpp
        class MockGenericParser:
            def __init__(self):
                self.language = Language(tscpp.language())
                self.parser = Parser(self.language)
        from cpp_parser_enhanced import CppTreeSitterParser

        generic_wrapper = MockGenericParser()
        cpp_parser = CppTreeSitterParser(generic_wrapper)
        
        print(f"\n Parsing: {test_file}")
        result = cpp_parser.parse(test_file)
        
        print("\n" + "=" * 60)
        print("PARSING RESULTS")
        print("=" * 60)
        
        print(f"\n File: {result['file_path']}")
        print(f" Language: {result['lang']}")
    
        print(f"\n Functions Found: {len(result['functions'])}")
        for func in result['functions']:
            print(f"\n  Function: {func['name']}")
            print(f"    Lines: {func['line_number']}-{func['end_line']}")
            print(f"    Return Type: {func['return_type']}")
            print(f"    Parameters: {len(func['args'])}")
            for param in func['args']:
                default = f" = {param['default']}" if param['default'] else ""
                print(f"      - {param['type']} {param['name']}{default}")
            print(f"    Modifiers: {', '.join(func['modifiers']) if func['modifiers'] else 'None'}")
            print(f"    Complexity: {func['complexity']}")
            print(f"    Calls: {[c['name'] for c in func['calls']]}")
            if func['docstring']:
                print(f"    Docstring: {func['docstring'][:50]}...")
        

        print(f"\n Classes Found: {len(result['classes'])}")
        for cls in result['classes']:
            print(f"\n  Class: {cls['name']}")
            print(f"    Lines: {cls['line_number']}-{cls['end_line']}")
            print(f"    Base Classes: {cls['bases']}")
            print(f"    Template: {cls['is_template']}")
            print(f"    Methods: {len(cls['methods'])}")
            for method in cls['methods']:
                print(f"      - {method['access']}: {method['return_type']} {method['name']}()")
            print(f"    Members: {len(cls['members'])}")
            for member in cls['members']:
                print(f"      - {member['access']}: {member['type']} {member['name']}")
            if cls['docstring']:
                print(f"    Docstring: {cls['docstring'][:50]}...")
        
        print(f"\n Namespaces Found: {len(result['namespaces'])}")
        for ns in result['namespaces']:
            print(f"  - {ns['name']} (lines {ns['line_number']}-{ns['end_line']})")
        
        print(f"\n Enums Found: {len(result['enums'])}")
        for enum in result['enums']:
            print(f"  - {enum['name']}: {enum['values']}")
        
        print(f"\n Global Variables: {len(result['variables'])}")
        for var in result['variables']:
            print(f"  - {var['type']} {var['name']} (line {var['line_number']})")
        
        print(f"\n Imports Found: {len(result['imports'])}")
        for imp in result['imports']:
            import_type = "system" if imp.get('is_system') else "local"
            print(f"  - {imp['name']} ({import_type})")
        
        print(f"\n Function Calls: {len(result['function_calls'])}")
        for call in result['function_calls'][:10]:
            print(f"  - {call['name']} (line {call['line_number']})")
        
        output_file = Path("test_result.json")
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        
        print(f"\n Full results saved to: {output_file}")
        print("\n" + "=" * 60)
        print("TEST COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        assert True
        
    except Exception as e:
        print(f"\n ERROR: {e}")
        import traceback
        traceback.print_exc()
        assert True
    
    finally:
        if test_file.exists():
            test_file.unlink()
        print(f"\n cleaned up test file")

if __name__ == "__main__":
    success = test_parser()
    sys.exit(0 if success else 1)
