import HeroSection from "@/components/HeroSection";
import FeaturesSection from "@/components/FeaturesSection";
import InstallationSection from "@/components/InstallationSection";
import DemoSection from "@/components/DemoSection";
import ExamplesSection from "@/components/ExamplesSection";
import CookbookSection from "@/components/CookbookSection";
import Footer from "@/components/Footer";
import TestimonialSection from "@/components/TestimonialSection";
import ComparisonTable from "@/components/ComparisonTable";

const Index = () => {
  return (
    <main className="min-h-screen">
      <HeroSection />
      <DemoSection />
      <ComparisonTable />
      <FeaturesSection />
      <InstallationSection />
      <ExamplesSection />
      <TestimonialSection />
      <CookbookSection />
      <Footer />
    </main>
  );
};

export default Index;
