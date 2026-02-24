'use client';

import { motion } from 'framer-motion';

interface HUDProps {
  drift: number;
}

export default function HUD({ drift }: HUDProps) {
  const isHealthy = drift < 3;
  const isWarning = drift >= 3 && drift < 10;
  const isCritical = drift >= 10;

  return (
    <div className="absolute top-0 left-0 w-full h-full pointer-events-none z-10">
      {/* Scanlines */}
      <div className="absolute inset-0 opacity-10 pointer-events-none"
        style={{
          backgroundImage: 'repeating-linear-gradient(0deg, transparent, transparent 2px, rgba(0,212,255,0.03) 2px, rgba(0,212,255,0.03) 4px)',
          backgroundSize: '100% 4px'
        }}
      />

      {/* Top status bar */}
      <div className="absolute top-6 left-6 right-6 flex justify-between font-mono text-sm">
        <motion.div 
          className={`flex items-center gap-4 ${isCritical ? 'text-red-500' : isWarning ? 'text-yellow-400' : 'text-cyan-400'}`}
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.5 }}
        >
          <span className="text-cyan-600">AGENTIC_OS_V1.0</span>
          <span className="text-cyan-700">|</span>
          <span>Drift: {drift.toFixed(1)}%</span>
          <span className={`w-2 h-2 rounded-full ${isHealthy ? 'bg-green-500' : isWarning ? 'bg-yellow-500' : 'bg-red-500'} animate-pulse`} />
        </motion.div>

        <motion.div 
          className="text-cyan-600"
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.7 }}
        >
          Runtime: 6 months
        </motion.div>
      </div>

      {/* Drift warning */}
      {isCritical && (
        <motion.div
          className="absolute top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 text-red-500 font-mono text-2xl"
          initial={{ opacity: 0, scale: 0.8 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ repeat: Infinity, duration: 2 }}
        >
          ⚠️ CRITICAL DRIFT DETECTED
        </motion.div>
      )}

      {/* Bottom metrics */}
      <div className="absolute bottom-6 left-6 right-6 flex justify-between font-mono text-xs text-cyan-700">
        <div>
          <div>Status: OPERATIONAL</div>
          <div>Subsystems: 11 ACTIVE</div>
          <div>Agents: ~200</div>
        </div>
        <div className="text-right">
          <div>Lines of Code: 200K+</div>
          <div>Yield: 66.7%</div>
          <div>Next cycle: ~4h</div>
        </div>
      </div>

      {/* Corner brackets */}
      <div className="absolute top-4 left-4 w-8 h-8 border-l-2 border-t-2 border-cyan-500/30" />
      <div className="absolute top-4 right-4 w-8 h-8 border-r-2 border-t-2 border-cyan-500/30" />
      <div className="absolute bottom-4 left-4 w-8 h-8 border-l-2 border-b-2 border-cyan-500/30" />
      <div className="absolute bottom-4 right-4 w-8 h-8 border-r-2 border-b-2 border-cyan-500/30" />
    </div>
  );
}
