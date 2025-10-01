import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

const tableData = [
  {
    feature: "Code Completion",
    copilot: { text: "Strong", status: "good" },
    cursor: { text: "Strong", status: "good" },
    cgc: { text: "Strong", status: "good" },
  },
  {
    feature: "Refactoring Suggestions",
    copilot: { text: "Limited to context length", status: "warning" },
    cursor: { text: "Limited to context length", status: "warning" },
    cgc: { text: "Via dependency tracing", status: "good" },
  },
  {
    feature: "Codebase Understanding",
    copilot: { text: "Limited", status: "bad" },
    cursor: { text: "Partial (local context)", status: "warning" },
    cgc: { text: "Deep graph-based", status: "good" },
  },
  {
    feature: "Call Graph & Imports",
    copilot: { text: "No", status: "bad" },
    cursor: { text: "No", status: "bad" },
    cgc: { text: "Direct + Multi-hops", status: "good" },
  },
  {
    feature: "Cross-File Tracing",
    copilot: { text: "Very low", status: "bad" },
    cursor: { text: "Some", status: "warning" },
    cgc: { text: "Complete code", status: "good" },
  },
  {
    feature: "LLM Explainability",
    copilot: { text: "Low", status: "bad" },
    cursor: { text: "Hallucinate", status: "warning" },
    cgc: { text: "Extremely good", status: "good" },
  },
  {
    feature: "Performance on Large Codebases",
    copilot: { text: "Slows with size", status: "bad" },
    cursor: { text: "Slows with size", status: "bad" },
    cgc: { text: "Scales with graph DB", status: "good" },
  },
  {
    feature: "Extensible to Multiple Languages",
    copilot: { text: "Strong", status: "good" },
    cursor: { text: "Strong", status: "good" },
    cgc: { text: "Work-in-progress", status: "warning" },
  },
  {
    feature: "Set-up Time for new code",
    copilot: { text: "Low", status: "good" },
    cursor: { text: "Slows with size", status: "bad" },
    cgc: { text: "Slows with size", status: "bad" },
  },
];

const StatusBadge = ({ status, text }: { status: string; text: string }) => {
  const getStatusStyles = () => {
    switch (status) {
      case "good":
        return "bg-emerald-500/10 text-emerald-400 border-emerald-500/20";
      case "warning":
        return "bg-amber-500/10 text-amber-400 border-amber-500/20";
      case "bad":
        return "bg-red-500/10 text-red-400 border-red-500/20";
      default:
        return "bg-secondary/50 text-muted-foreground";
    }
  };

  const getIcon = () => {
    switch (status) {
      case "good":
        return "✅";
      case "warning":
        return "⚠️";
      case "bad":
        return "❌";
      default:
        return "";
    }
  };

  return (
    <Badge className={`${getStatusStyles()} border font-normal text-xs px-2 py-1`}>
      <span className="mr-1.5">{getIcon()}</span>
      {text}
    </Badge>
  );
};

export default function ComparisonTable() {
  return (
    <section className="py-20 px-4 bg-gradient-to-b from-secondary/10 to-background">
      <div className="container mx-auto max-w-6xl">
        <div className="text-center mb-12">
          <h2 className="text-3xl sm:text-4xl md:text-5xl font-bold mb-4 pb-4 bg-gradient-to-r from-primary via-primary to-accent bg-clip-text text-transparent">
            Why CodeGraphContext?
          </h2>
          <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
            See how CodeGraphContext compares to other popular AI coding assistants
          </p>
        </div>

        <Card className="bg-gradient-card border-border/50 overflow-hidden">
          <CardContent className="p-0">
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border/50 bg-secondary/30">
                    <th className="p-4 sm:p-6 text-left font-semibold text-foreground text-sm sm:text-base">
                      Feature
                    </th>
                    <th className="p-4 sm:p-6 text-center font-semibold text-foreground text-sm sm:text-base min-w-[140px]">
                      GitHub Copilot
                    </th>
                    <th className="p-4 sm:p-6 text-center font-semibold text-foreground text-sm sm:text-base min-w-[140px]">
                      Cursor
                    </th>
                    <th className="p-4 sm:p-6 text-center font-semibold text-primary text-sm sm:text-base min-w-[140px]">
                      CodeGraphContext
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {tableData.map((row, index) => (
                    <tr
                      key={row.feature}
                      className={`border-b border-border/30 transition-colors hover:bg-secondary/20 ${
                        index % 2 === 0 ? "bg-background/50" : "bg-secondary/5"
                      }`}
                    >
                      <td className="p-4 sm:p-6 text-foreground font-medium text-sm sm:text-base">
                        {row.feature}
                      </td>
                      <td className="p-4 sm:p-6 text-center">
                        <div className="flex justify-center">
                          <StatusBadge status={row.copilot.status} text={row.copilot.text} />
                        </div>
                      </td>
                      <td className="p-4 sm:p-6 text-center">
                        <div className="flex justify-center">
                          <StatusBadge status={row.cursor.status} text={row.cursor.text} />
                        </div>
                      </td>
                      <td className="p-4 sm:p-6 text-center">
                        <div className="flex justify-center">
                          <StatusBadge status={row.cgc.status} text={row.cgc.text} />
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>

        <div className="text-center mt-8">
          <p className="text-sm text-muted-foreground">
            Experience the power of graph-based code understanding
          </p>
        </div>
      </div>
    </section>
  );
}