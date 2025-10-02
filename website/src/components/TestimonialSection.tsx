import { OrbitingCircles } from "@/components/ui/orbiting-circles"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { ChevronLeft, ChevronRight } from "lucide-react"
import { useMemo, useState } from "react"

const avatars = [
  { imageUrl: "https://avatars.githubusercontent.com/u/161715841?s=400&u=e766c0710c29e4f5913698aa01c08531ef049775&v=4", profileUrl: "https://github.com/athaxv" },
  { imageUrl: "https://avatars.githubusercontent.com/u/20110627", profileUrl: "https://github.com/tomonarifeehan" },
  { imageUrl: "https://avatars.githubusercontent.com/u/106103625", profileUrl: "https://github.com/BankkRoll" },
  { imageUrl: "https://avatars.githubusercontent.com/u/59228569", profileUrl: "https://github.com/safethecode" },
  { imageUrl: "https://avatars.githubusercontent.com/u/59442788", profileUrl: "https://github.com/sanjay-mali" },
  { imageUrl: "https://avatars.githubusercontent.com/u/89768406", profileUrl: "https://github.com/itsarghyadas" },
]

export default function TestimonialSection() {
  const reviews = useMemo(
    () => [
      {
        quote:
          "We’ve cut our task completion time in half since adopting CodeGraphContext. The DX is phenomenal.",
        author: "Sarah L.",
        role: "Operations Manager",
      },
      {
        quote:
          "Querying code relationships is finally effortless. Our team relies on it daily.",
        author: "Miguel A.",
        role: "Tech Lead",
      },
      {
        quote:
          "The visual graph helps new engineers onboard dramatically faster.",
        author: "Priya K.",
        role: "Engineering Manager",
      },
      {
        quote:
          "Rock‑solid performance, great documentation, and fantastic support.",
        author: "Daniel W.",
        role: "Staff Engineer",
      },
    ],
    []
  )
  const [index, setIndex] = useState(0)
  const next = () => setIndex((i) => (i + 1) % reviews.length)
  const prev = () => setIndex((i) => (i - 1 + reviews.length) % reviews.length)

  return (
    <section className="py-24 px-4">
      <div className="container mx-auto max-w-6xl">
        <div className="text-center mb-16">
          <h2 className="text-4xl md:text-5xl font-bold mb-6 bg-gradient-primary bg-clip-text text-transparent">
            What teams are saying
          </h2>
          <p className="text-xl text-muted-foreground max-w-3xl mx-auto">
            Real feedback from engineers and leaders using CodeGraphContext
          </p>
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-12 items-center">
        {/* Left: Orbiting avatars */}
        <div className="relative mx-auto h-[500px] w-full flex items-center justify-center overflow-hidden">
      {/* Outer orbit */}
      <OrbitingCircles iconSize={56} radius={180} speed={1.4}>
        {avatars.slice(0, 5).map((avatar, index) => (
          <a key={index} href={avatar.profileUrl} target="_blank" rel="noopener noreferrer">
            <img
              src={avatar.imageUrl}
              alt={`avatar-${index}`}
              width={56}
              height={56}
              className="rounded-full border-2 border-white shadow-md dark:border-neutral-800"
            />
          </a>
        ))}
      </OrbitingCircles>

      {/* Inner orbit */}
      <OrbitingCircles iconSize={44} radius={100} reverse speed={2}>
        {avatars.slice(1, 5).map((avatar, index) => (
          <a key={index} href={avatar.profileUrl} target="_blank" rel="noopener noreferrer">
            <img
              src={avatar.imageUrl}
              alt={`avatar-inner-${index}`}
              width={44}
              height={44}
              className="rounded-full border-2 border-white shadow-md dark:border-neutral-800"
            />
          </a>
        ))}
      </OrbitingCircles>

      {/* Center avatar */}
      <a
        href={avatars[0].profileUrl}
        target="_blank"
        rel="noopener noreferrer"
        className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2"
      >
        <img
          src={avatars[0].imageUrl}
          alt="center-avatar"
          width={72}
          height={72}
          className="rounded-full border-2 border-white shadow-lg dark:border-neutral-800"
        />
      </a>
        </div>

        {/* Right: Testimonial content */}
        <Card className="dark:bg-gradient-card dark:bg-card/50 dark:border-border/30 bg-white/95 border-gray-200/50 shadow-sm">
          <CardHeader>
            <CardTitle className="text-3xl md:text-4xl font-bold bg-gradient-primary bg-clip-text text-transparent">
              Teams love the experience
            </CardTitle>
            <CardDescription className="text-base md:text-lg dark:text-muted-foreground text-gray-600">
              “{reviews[index].quote}”
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-between gap-4">
              <div>
                <p className="font-semibold">{reviews[index].author}</p>
                <p className="text-sm text-muted-foreground">{reviews[index].role}</p>
              </div>
              <div className="flex gap-2">
                <Button onClick={prev} size="icon" variant="outline" className="bg-white/80 backdrop-blur-sm border-gray-300 shadow-sm dark:bg-transparent dark:border-border">
                  <ChevronLeft className="h-4 w-4" />
                </Button>
                <Button onClick={next} size="icon" className="bg-gradient-to-r from-purple-600 via-indigo-600 to-purple-800 text-primary-foreground shadow-glow ring-1 ring-primary/20 dark:bg-gradient-primary">
                  <ChevronRight className="h-4 w-4" />
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
        </div>
      </div>
    </section>
  )
}
