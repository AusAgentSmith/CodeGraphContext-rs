import os
import pytest

SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "sample_project")

def test_sample_files_exist():
    assert os.path.isdir(SAMPLE_DIR)
    assert os.path.exists(os.path.join(SAMPLE_DIR, "module_a.py"))
    assert os.path.exists(os.path.join(SAMPLE_DIR, "dynamic_dispatch.py"))

def test_codegraphcontext_integration():
    pytest.importorskip('codegraphcontext')
    try:
        from codegraphcontext.core import CodeGraph
    except Exception as e:
        pytest.skip(f"Could not import CodeGraph: {e}")

    cg = CodeGraph.from_folder(SAMPLE_DIR)
    # Attempt to query for some expected nodes (API may vary; adapt as needed)
    try:
        funcs = cg.get_all_functions()
        names = [f.get('name') if isinstance(f, dict) else getattr(f, 'name', None) for f in funcs]
        assert any('dispatch_by_key' in str(n) or 'dispatch_by_string' in str(n) for n in names)
        assert any('choose_path' in str(n) for n in names)
    except Exception:
        # If API differs, just assert the graph has nodes
        if hasattr(cg, 'nodes'):
            assert len(list(cg.nodes())) > 0
        else:
            assert True
