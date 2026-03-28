import ForceGraph2D from "react-force-graph-2d";
import { useCallback, useRef, useState, useEffect, useMemo } from "react";
import { Button } from "@/components/ui/button";
import { 
  ArrowLeft, ZoomIn, ZoomOut, Maximize, FileCode, Search, 
  ChevronRight, Eye, EyeOff, Settings2, Palette, Github, Star
} from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";

const DEFAULT_NODE_COLORS: Record<string, string> = {
  Repository: '#ffffff',
  Folder: '#f59e0b',
  File: '#42a5f5',
  Class: '#66bb6a',
  Interface: '#26a69a',
  Trait: '#81c784',
  Function: '#ffca28',
  Module: '#ef5350',
  Variable: '#ffa726',
  Enum: '#7e57c2',
  Struct: '#5c6bc0',
  Annotation: '#ec407a',
  Parameter: '#90a4ae',
  Other: '#78909c'
};

const DEFAULT_EDGE_COLORS: Record<string, string> = {
  CONTAINS: '#ffffff',
  CALLS: '#ab47bc',
  IMPORTS: '#42a5f5',
  INHERITS: '#66bb6a',
  HAS_PARAMETER: '#ffca28'
};

export default function CodeGraphViewer({ data, onClose }: { data: any, onClose: () => void }) {
  const fgRef = useRef<any>();
  const [dimensions, setDimensions] = useState({ width: window.innerWidth, height: window.innerHeight });
  const [hoverNode, setHoverNode] = useState<any>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedFile, setSelectedFile] = useState<string | null>(null);
  const [focusSet, setFocusSet] = useState<{nodes: Set<number>, links: Set<any>} | null>(null);

  // LEGEND & CONFIG STATE
  const [nodeColors, setNodeColors] = useState(DEFAULT_NODE_COLORS);
  const [edgeColors, setEdgeColors] = useState(DEFAULT_EDGE_COLORS);
  const [visibleNodeTypes, setVisibleNodeTypes] = useState<Set<string>>(() => {
    // Automatically disable Variables by default as requested
    const all = new Set(Object.keys(DEFAULT_NODE_COLORS));
    all.delete('Variable');
    all.delete('Parameter');
    return all;
  });
  const [showConfig, setShowConfig] = useState(false);
  const [lineWidth, setLineWidth] = useState(0.8);

  useEffect(() => {
    const handleResize = () => setDimensions({ 
      width: window.innerWidth, 
      height: window.innerHeight 
    });
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  // MEMORY PROFILING INJECTION
  useEffect(() => {
    const memoryInterval = setInterval(() => {
      const pm = (performance as any).memory;
      if (pm) {
        console.log(
          `%c[RAM Profile] Used: ${(pm.usedJSHeapSize / 1048576).toFixed(2)} MB ` +
          `| Total: ${(pm.totalJSHeapSize / 1048576).toFixed(2)} MB ` +
          `| Limit: ${(pm.jsHeapSizeLimit / 1048576).toFixed(2)} MB`,
          'color: #00ff00; font-weight: bold; background: #000; padding: 2px 4px;'
        );
      } else {
        console.log('[RAM Profile] performance.memory is not supported in this browser.');
      }
    }, 1000);

    return () => clearInterval(memoryInterval);
  }, []);

  const getRGBA = (hex: string, alpha: number) => {
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  };

  const nodeCanvasObject = useCallback((node: any, ctx: any, globalScale: number) => {
    if (!visibleNodeTypes.has(node.type)) return;

    const isHovered = hoverNode && node.id === hoverNode.id;
    const isFocused = focusSet ? focusSet.nodes.has(node.id) : true;
    
    const baseColor = nodeColors[node.type] || nodeColors.Other;
    const radius = node.val * 0.8;
    const opacity = isFocused ? (isHovered ? 1 : 0.9) : 0.05;

    const isMassive = data.nodes && data.nodes.length > 3000;

    // Hyper-optimized rendering for vast dimmed background graphs
    if (isMassive && !isFocused && !isHovered) {
       ctx.fillStyle = getRGBA(baseColor, opacity);
       // fillRect is 10x faster than arc for Canvas
       ctx.fillRect(node.x - radius, node.y - radius, radius * 2, radius * 2);
       return;
    }

    // Security check: Ignore NaN physics blowouts to prevent GPU crash
    if (!Number.isFinite(node.x) || !Number.isFinite(node.y) || !Number.isFinite(radius)) return;

    // Draw Glow/Halo
    if (isHovered || (selectedFile && node.file === selectedFile && node.type === 'File')) {
       ctx.beginPath();
       ctx.arc(node.x, node.y, radius * (isHovered ? 2.5 : 2.0), 0, 2 * Math.PI, false);
       ctx.fillStyle = getRGBA(baseColor, isHovered ? (isFocused ? 0.3 : 0.1) : 0.1);
       ctx.fill();
    }

    // Draw Node Circle
    ctx.beginPath();
    ctx.arc(node.x, node.y, radius, 0, 2 * Math.PI, false);
    ctx.fillStyle = isFocused ? baseColor : getRGBA(baseColor, opacity);
    ctx.fill();

    // Labels: Disable for huge non-hovered graphs unless very zoomed in
    if (isHovered || (isFocused && globalScale > (isMassive ? 5.0 : 2.0))) {
      // CRITICAL: Font sizes must be rounded to integers!
      // Assigning fractional font sizes (14.21851px) millions of times per second permanently exhausts Chrome's Font Glyph Cache Buffer and causes a hard SIGKILL Aw Snap!
      const fontSize = Math.max(2, Math.round((isHovered ? 14 : 10) / globalScale));
      
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillStyle = isFocused ? 'rgba(255, 255, 255, 0.9)' : 'rgba(255, 255, 255, 0.1)';
      ctx.font = `${isHovered ? 'bold' : 'normal'} ${fontSize}px Inter, sans-serif`;
      
      if (isFocused && !isMassive) {
        ctx.shadowColor = 'black';
        ctx.shadowBlur = 4;
      }
      ctx.fillText(node.name || 'Unknown', node.x, node.y + radius + (fontSize/2) + 4);
      if (isFocused && !isMassive) ctx.shadowBlur = 0; // Only reset if we actually set it
    }
  }, [hoverNode, selectedFile, nodeColors, visibleNodeTypes, focusSet, data]);

  const handleZoom = (inOut: number) => {
    fgRef.current?.zoom(fgRef.current.zoom() * inOut, 400);
  };

  const toggleNodeType = (type: string) => {
    const next = new Set(visibleNodeTypes);
    if (next.has(type)) next.delete(type);
    else next.add(type);
    setVisibleNodeTypes(next);
  };

  const filteredFiles = useMemo(() => {
    if (!data.files) return [];
    return data.files.filter((f: string) => f.toLowerCase().includes(searchQuery.toLowerCase()));
  }, [data.files, searchQuery]);

  const filteredData = useMemo(() => {
    const pmStart = (performance as any).memory;
    console.log(`[CodeGraph] Starting filteredData calculation. RAM: ${pmStart ? (pmStart.usedJSHeapSize/1048576).toFixed(1) : '?'} MB`);

    const visibleNodes = data.nodes.filter((n: any) => visibleNodeTypes.has(n.type));
    const nodeIds = new Set(visibleNodes.map((n: any) => n.id));
    const visibleLinks = data.links.filter((l: any) => 
      nodeIds.has(typeof l.source === 'object' ? l.source.id : l.source) && 
      nodeIds.has(typeof l.target === 'object' ? l.target.id : l.target)
    );
    
    const pmEnd = (performance as any).memory;
    console.log(`[CodeGraph] finished filteredData calculation. RAM: ${pmEnd ? (pmEnd.usedJSHeapSize/1048576).toFixed(1) : '?'} MB. nodes=${visibleNodes.length}, links=${visibleLinks.length}`);
    return { nodes: visibleNodes, links: visibleLinks };
  }, [data, visibleNodeTypes]);

  const onFileClick = (path: string | null) => {
    if (!path) {
      setSelectedFile(null);
      setFocusSet(null);
      return;
    }

    setSelectedFile(path);
    const fileNode = data.nodes.find((n: any) => n.file === path && n.type === 'File');
    if (fileNode) {
      if (fgRef.current) {
        fgRef.current.centerAt(fileNode.x, fileNode.y, 800);
        fgRef.current.zoom(2.5, 800);
      }

      // Calculate Focus Set: The node + all its descendants/connections
      const nodesInFocus = new Set<number>();
      const linksInFocus = new Set<any>();
      
      nodesInFocus.add(fileNode.id);
      
      const pm = (performance as any).memory;
      console.log(`[CodeGraph] Processing Focus Set for ${fileNode.name}. Before traversal: ${pm ? (pm.usedJSHeapSize/1048576).toFixed(1) : '?'}MB used.`);

      // Basic 1-level traversal for now (contains, imports, etc.)
      data.links.forEach((l: any) => {
        const sId = typeof l.source === 'object' ? l.source.id : l.source;
        const tId = typeof l.target === 'object' ? l.target.id : l.target;
        
        if (sId === fileNode.id || tId === fileNode.id) {
          nodesInFocus.add(sId);
          nodesInFocus.add(tId);
          linksInFocus.add(l);
        }
      });
      
      console.log(`[CodeGraph] Focus traversal complete. After traversal: ${pm ? (pm.usedJSHeapSize/1048576).toFixed(1) : '?'}MB used.`);

      setFocusSet({ nodes: nodesInFocus, links: linksInFocus });
    }
  };

  const getLinkColor = useCallback((link: any) => {
     const isFocused = focusSet ? focusSet.links.has(link) : true;
     const baseColor = edgeColors[link.type] || 'rgba(255,255,255,0.1)';
     
     // CRITICAL OPTIMIZATION: Return static string to bypass getRGBA execution for 200,000 links every 60fps frame!
     // Slicing 6 strings per edge * 200k edges * 60 fps = 72 Million string allocations per SECOND! (OOM Crash Cause)
     if (!isFocused) return 'rgba(255, 255, 255, 0.02)';
     return baseColor;
  }, [focusSet, edgeColors]);

  return (
    <motion.div 
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="fixed inset-0 z-50 bg-[#020202] overflow-hidden flex font-sans"
    >
      {/* SIDEBAR */}
      <div className="w-80 h-full bg-black/60 backdrop-blur-3xl border-r border-white/10 z-[70] flex flex-col shadow-2xl overflow-hidden">
        <div className="p-6 pb-2">
           <Button 
            onClick={onClose} 
            variant="ghost" 
            className="w-full justify-start text-gray-400 hover:text-white hover:bg-white/5 mb-6 rounded-xl border border-white/5 transition-colors"
          >
            <ArrowLeft className="w-4 h-4 mr-2" />
            Back to Dashboard
          </Button>
          
          <div className="flex items-center justify-between mb-2">
            <h2 className="text-xl font-bold text-white flex items-center gap-2 tracking-tight">
              <FileCode className="w-5 h-5 text-blue-400" />
              Project Tree
            </h2>
            <button onClick={() => setShowConfig(!showConfig)} title="Graph Settings" className={`p-2 rounded-lg transition-colors ${showConfig ? 'bg-blue-500/20 text-blue-400' : 'text-gray-500 hover:text-white'}`}>
              <Settings2 className="w-5 h-5" />
            </button>
          </div>
          
          <div className="relative mb-4">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500" />
            <input 
              type="text" 
              placeholder="Filter files..." 
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full bg-white/5 border border-white/10 rounded-xl py-2 pl-10 pr-4 text-sm text-white focus:outline-none focus:ring-1 focus:ring-blue-500/50 transition-all font-medium"
            />
          </div>
        </div>

        <div className="flex-1 overflow-y-auto px-3 custom-scrollbar">
           {showConfig ? (
             <motion.div initial={{ opacity: 0, x: -10 }} animate={{ opacity: 1, x: 0 }} className="p-3 space-y-6">
                <div>
                   <h3 className="text-xs font-bold text-gray-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                      <Palette className="w-3 h-3" /> Visualization Config
                   </h3>
                   
                   <div className="mb-6 px-1">
                      <label className="text-[10px] text-gray-400 uppercase font-bold tracking-widest block mb-2">Edge Width: {lineWidth.toFixed(1)}px</label>
                      <input 
                        type="range" 
                        min="0.2" 
                        max="3.0" 
                        step="0.1" 
                        value={lineWidth} 
                        onChange={(e) => setLineWidth(parseFloat(e.target.value))}
                        className="w-full accent-blue-500 h-1 bg-white/10 rounded-lg appearance-none cursor-pointer"
                      />
                   </div>

                   <div className="space-y-3">
                      {Object.keys(DEFAULT_NODE_COLORS).map(type => (
                        <div key={type} className="flex items-center justify-between group">
                           <div className="flex items-center gap-3">
                              <button 
                                onClick={() => toggleNodeType(type)}
                                className={`p-1 rounded transition-colors ${visibleNodeTypes.has(type) ? 'text-blue-400 bg-blue-500/10' : 'text-gray-600'}`}
                              >
                                {visibleNodeTypes.has(type) ? <Eye className="w-4 h-4" /> : <EyeOff className="w-4 h-4" />}
                              </button>
                              <span className={`text-sm ${visibleNodeTypes.has(type) ? 'text-gray-200' : 'text-gray-600'}`}>{type}</span>
                           </div>
                           <input 
                             type="color" 
                             value={nodeColors[type] || '#78909c'} 
                             onChange={(e) => setNodeColors({...nodeColors, [type]: e.target.value})}
                             className="w-6 h-6 bg-transparent border-none cursor-pointer p-0 rounded overflow-hidden"
                           />
                        </div>
                      ))}
                   </div>
                </div>

                <div>
                   <h3 className="text-xs font-bold text-gray-500 uppercase tracking-widest mb-4">Edge Type Colors</h3>
                   <div className="space-y-3">
                      {Object.keys(DEFAULT_EDGE_COLORS).map(type => (
                        <div key={type} className="flex items-center justify-between">
                           <span className="text-sm text-gray-400">{type}</span>
                           <input 
                             type="color" 
                             value={edgeColors[type]} 
                             onChange={(e) => setEdgeColors({...edgeColors, [type]: e.target.value})}
                             className="w-6 h-6 bg-transparent border-none cursor-pointer p-0"
                           />
                        </div>
                      ))}
                   </div>
                </div>
             </motion.div>
           ) : (
             <div className="space-y-1">
               {filteredFiles.map((file: string) => (
                 <button
                   key={file}
                   onClick={() => onFileClick(file)}
                   className={`w-full text-left px-3 py-2 rounded-xl text-sm transition-all flex items-center gap-3 group ${
                     selectedFile === file 
                     ? 'bg-blue-500/20 text-blue-200 border border-blue-500/30' 
                     : 'text-gray-400 hover:text-white hover:bg-white/5 border border-transparent'
                   }`}
                 >
                   <div className={`w-1.5 h-1.5 rounded-full transition-shadow duration-300 ${selectedFile === file ? 'bg-blue-400 shadow-[0_0_10px_#3b82f6]' : 'bg-gray-600 group-hover:bg-gray-400'}`} />
                   <span className="truncate flex-1 font-medium">{file.split('/').pop()}</span>
                 </button>
               ))}
             </div>
           )}
        </div>

        <div className="p-6 border-t border-white/5 bg-black/40 text-[10px] text-gray-400 flex justify-between uppercase tracking-widest font-black">
           <span>{filteredData.nodes.length} Visible</span>
           <span>{filteredData.links.length} Edges</span>
        </div>
      </div>

      {/* VIEWPORT */}
      <div className="flex-1 relative bg-[radial-gradient(circle_at_center,_#0a0a0a_0%,_#000_100%)]">
        
        {/* Top Right Badges */}
        <div className="absolute top-6 right-6 z-[60] flex flex-col md:flex-row items-end md:items-center gap-3">
           <a 
             href="https://github.com/CodeGraphContext/CodeGraphContext"
             target="_blank"
             rel="noopener noreferrer"
             className="flex items-center gap-2 bg-black/40 hover:bg-white/10 text-white text-[11px] uppercase tracking-widest font-bold px-4 py-2 border border-white/10 rounded-full transition-all backdrop-blur-md shadow-2xl"
           >
             <Star className="w-3.5 h-3.5 text-yellow-400 fill-yellow-400" /> 
             Star on GitHub
           </a>
           <div className="bg-black/40 text-gray-400 text-[11px] uppercase tracking-widest font-bold px-4 py-2 border border-white/10 rounded-full backdrop-blur-md shadow-2xl">
             Made by <a href="https://github.com/shashankss1205" target="_blank" rel="noopener noreferrer" className="text-blue-400 hover:text-blue-300 transition-colors">shashankss1205</a>
           </div>
        </div>

        <div className="absolute top-6 left-6 z-[60] flex flex-col gap-4">
          <div className="flex flex-col bg-black/60 border border-white/10 backdrop-blur-xl rounded-2xl overflow-hidden shadow-2xl">
            <button onClick={() => handleZoom(1.4)} className="p-3 hover:bg-white/10 text-gray-300 transition-colors border-b border-white/5"><ZoomIn className="w-5 h-5" /></button>
            <button onClick={() => fgRef.current?.zoomToFit(600, 100)} className="p-3 hover:bg-white/10 text-gray-300 transition-colors border-b border-white/5"><Maximize className="w-5 h-5" /></button>
            <button onClick={() => handleZoom(0.7)} className="p-3 hover:bg-white/10 text-gray-300 transition-colors"><ZoomOut className="w-5 h-5" /></button>
          </div>
          
          <AnimatePresence>
            {selectedFile && (
              <motion.button 
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.9 }}
                onClick={() => onFileClick(null)}
                className="bg-red-500/20 hover:bg-red-500/40 text-red-400 border border-red-500/30 text-xs font-bold uppercase tracking-widest py-3 px-5 rounded-xl backdrop-blur-xl transition-all shadow-xl"
              >
                Clear Focus
              </motion.button>
            )}
          </AnimatePresence>
        </div>

        <ForceGraph2D
          ref={fgRef}
          graphData={filteredData}
          width={dimensions.width - 320}
          height={dimensions.height}
          nodeLabel="name"
          linkColor={getLinkColor}
          linkWidth={lineWidth}
          linkDirectionalParticles={(l: any) => (focusSet ? (focusSet.links.has(l) ? 2 : 0) : (filteredData.links.length > 500 ? 0 : 1))}
          linkDirectionalParticleWidth={lineWidth * 1.5}
          linkDirectionalParticleSpeed={0.005}
          nodeCanvasObject={nodeCanvasObject}
          onNodeClick={(node: any) => {
             if (node.type === 'File') onFileClick(node.file);
          }}
          onBackgroundClick={() => onFileClick(null)}
          onNodeHover={setHoverNode}
          d3VelocityDecay={0.4}
          d3AlphaDecay={0.05}
          cooldownTicks={50}
        />

        {/* Legend Overlay (Full Clickable) */}
        {!showConfig && (
          <div 
            onClick={() => setShowConfig(true)}
            className="absolute bottom-6 right-6 z-[60] bg-black/50 hover:bg-black/70 transition-colors cursor-pointer backdrop-blur-3xl border border-white/10 rounded-2xl p-5 shadow-2xl max-w-lg"
          >
             <p className="text-[10px] text-gray-500 font-bold uppercase tracking-widest mb-3 flex items-center justify-between">
               <span>Graph Legend</span>
               <span className="text-blue-400/50">Click to Open Filters</span>
             </p>
             <div className="flex flex-wrap gap-x-5 gap-y-3 justify-end">
                {Object.keys(DEFAULT_NODE_COLORS).map(type => (
                  <div key={type} className="flex items-center gap-2">
                     <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: nodeColors[type], boxShadow: `0 0 8px ${nodeColors[type]}` }} />
                     <span className={`text-[10px] font-bold uppercase tracking-widest ${visibleNodeTypes.has(type) ? 'text-gray-300' : 'text-gray-600 line-through'}`}>
                       {type}
                     </span>
                  </div>
                ))}
             </div>
          </div>
        )}
      </div>
    </motion.div>
  );
}
