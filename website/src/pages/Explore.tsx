import { useState } from "react";
import CodeGraphViewer from "../components/CodeGraphViewer";
import LocalUploader from "../components/LocalUploader";
import { motion, AnimatePresence } from "framer-motion";

const Explore = () => {
  const [graphData, setGraphData] = useState<any>(null);

  return (
    <main className="min-h-screen bg-background pt-24 pb-12 px-6 flex flex-col items-center">
      <AnimatePresence mode="wait">
        {!graphData ? (
          <motion.div 
            key="uploader"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.95 }}
            className="w-full max-w-4xl mx-auto flex flex-col items-center mt-12"
          >
            <div className="text-center mb-12">
              <h1 className="text-4xl md:text-5xl font-bold mb-4 bg-gradient-to-r from-blue-400 to-indigo-500 bg-clip-text text-transparent">
                Graph Explorer
              </h1>
              <p className="text-muted-foreground text-lg max-w-2xl mx-auto">
                Scan a local directory, ZIP file, or public GitHub repository to instantly visualize its architecture in a highly interactive 2D node map. Complete privacy – all parsing is executed securely via WebAssembly natively within your browser.
              </p>
            </div>
            
            <div className="w-full max-w-2xl">
              <LocalUploader onComplete={setGraphData} />
            </div>
          </motion.div>
        ) : (
          <CodeGraphViewer 
            key="viewer" 
            data={graphData} 
            onClose={() => setGraphData(null)} 
          />
        )}
      </AnimatePresence>
    </main>
  );
};

export default Explore;
