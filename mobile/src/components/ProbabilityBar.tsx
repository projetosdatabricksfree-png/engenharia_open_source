import React from 'react';
import { View, Text, StyleSheet } from 'react-native';
import { COLORS, SPACING, RADIUS } from '../theme';

interface Props {
  homePct: number;
  drawPct: number;
  awayPct: number;
  predicted: string;
}

const BARS = [
  { key: 'casa',      label: 'Casa',   color: COLORS.primary },
  { key: 'empate',    label: 'Empate', color: COLORS.warning },
  { key: 'visitante', label: 'Fora',   color: COLORS.info },
] as const;

export default function ProbabilityBar({ homePct, drawPct, awayPct, predicted }: Props) {
  const values: Record<string, number> = { casa: homePct, empate: drawPct, visitante: awayPct };

  return (
    <View style={styles.container}>
      {BARS.map(({ key, label, color }) => {
        const pct = values[key] ?? 0;
        const isWinner = predicted === key;
        return (
          <View key={key} style={styles.barGroup}>
            <View style={styles.labelRow}>
              <Text style={[styles.label, isWinner && { color, fontWeight: '700' }]}>
                {label}
              </Text>
              <Text style={[styles.pct, isWinner && { color, fontWeight: '700' }]}>
                {pct.toFixed(1)}%
              </Text>
            </View>
            <View style={styles.track}>
              <View
                style={[
                  styles.fill,
                  {
                    backgroundColor: color,
                    width: `${Math.min(pct, 100)}%`,
                    opacity: isWinner ? 1 : 0.6,
                  },
                ]}
              />
            </View>
          </View>
        );
      })}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    gap: SPACING.xs,
    marginTop: SPACING.sm,
  },
  barGroup: { gap: 3 },
  labelRow: { flexDirection: 'row', justifyContent: 'space-between' },
  label: {
    fontSize: 11,
    color: COLORS.textSecondary,
    letterSpacing: 0.5,
    textTransform: 'uppercase',
  },
  pct: {
    fontSize: 11,
    color: COLORS.textSecondary,
    fontWeight: '600',
  },
  track: {
    height: 6,
    backgroundColor: COLORS.bgBorder,
    borderRadius: RADIUS.full,
    overflow: 'hidden',
  },
  fill: {
    height: '100%',
    borderRadius: RADIUS.full,
  },
});
