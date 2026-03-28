/**
 * Simple, lightweight line chart using react-native-svg.
 * No external charting library required beyond react-native-svg.
 */
import React from 'react';
import { View, Text, StyleSheet } from 'react-native';
import Svg, { Path, Line, Text as SvgText, Circle, Defs, LinearGradient, Stop, Rect } from 'react-native-svg';
import { COLORS, SPACING } from '../theme';

interface DataPoint {
  x: number;
  y: number;
}

interface Series {
  data: DataPoint[];
  color: string;
  dashed?: boolean;
  label?: string;
}

interface Props {
  series: Series[];
  width: number;
  height?: number;
  yLabel?: string;
  xLabel?: string;
  yFormat?: (v: number) => string;
}

const PAD = { top: 16, right: 16, bottom: 36, left: 44 };

export default function LineChart({ series, width, height = 200, yFormat, xLabel }: Props) {
  const chartW = width - PAD.left - PAD.right;
  const chartH = height - PAD.top - PAD.bottom;

  // Aggregate all points
  const allX = series.flatMap(s => s.data.map(p => p.x));
  const allY = series.flatMap(s => s.data.map(p => p.y));
  if (allX.length === 0) return null;

  const minX = Math.min(...allX);
  const maxX = Math.max(...allX);
  const minY = Math.max(0, Math.min(...allY) - 5);
  const maxY = Math.min(100, Math.max(...allY) + 5);

  const toSvgX = (x: number) =>
    allX.length === 1 ? chartW / 2 : ((x - minX) / (maxX - minX || 1)) * chartW;
  const toSvgY = (y: number) =>
    chartH - ((y - minY) / (maxY - minY || 1)) * chartH;

  const makePath = (pts: DataPoint[]) => {
    if (pts.length === 0) return '';
    const sorted = [...pts].sort((a, b) => a.x - b.x);
    return sorted
      .map((p, i) => `${i === 0 ? 'M' : 'L'} ${toSvgX(p.x).toFixed(1)} ${toSvgY(p.y).toFixed(1)}`)
      .join(' ');
  };

  // Y-axis ticks
  const yTicks = 4;
  const yTickVals = Array.from({ length: yTicks + 1 }, (_, i) =>
    minY + (i * (maxY - minY)) / yTicks
  );

  // X-axis ticks (up to 8)
  const uniqueX = [...new Set(allX)].sort((a, b) => a - b);
  const step = Math.ceil(uniqueX.length / 8);
  const xTickVals = uniqueX.filter((_, i) => i % step === 0);

  return (
    <View>
      <Svg width={width} height={height}>
        {/* Background grid */}
        {yTickVals.map((v, i) => {
          const y = toSvgY(v);
          return (
            <React.Fragment key={i}>
              <Line
                x1={PAD.left}
                y1={PAD.top + y}
                x2={PAD.left + chartW}
                y2={PAD.top + y}
                stroke={COLORS.bgBorder}
                strokeWidth={0.5}
              />
              <SvgText
                x={PAD.left - 4}
                y={PAD.top + y + 4}
                textAnchor="end"
                fontSize={9}
                fill={COLORS.textMuted}
              >
                {yFormat ? yFormat(v) : `${Math.round(v)}`}
              </SvgText>
            </React.Fragment>
          );
        })}

        {/* X-axis ticks */}
        {xTickVals.map((v, i) => (
          <SvgText
            key={i}
            x={PAD.left + toSvgX(v)}
            y={PAD.top + chartH + 14}
            textAnchor="middle"
            fontSize={9}
            fill={COLORS.textMuted}
          >
            {v}
          </SvgText>
        ))}

        {/* X-axis label */}
        {xLabel && (
          <SvgText
            x={PAD.left + chartW / 2}
            y={height - 4}
            textAnchor="middle"
            fontSize={9}
            fill={COLORS.textMuted}
          >
            {xLabel}
          </SvgText>
        )}

        {/* Series lines */}
        {series.map((s, si) => {
          const sorted = [...s.data].sort((a, b) => a.x - b.x);
          const pathD = makePath(sorted);
          return (
            <React.Fragment key={si}>
              <Path
                d={pathD}
                fill="none"
                stroke={s.color}
                strokeWidth={2}
                strokeDasharray={s.dashed ? '6,3' : undefined}
                transform={`translate(${PAD.left},${PAD.top})`}
                opacity={0.9}
              />
              {/* Dots on last point */}
              {sorted.length > 0 && (() => {
                const last = sorted[sorted.length - 1];
                return (
                  <Circle
                    cx={PAD.left + toSvgX(last.x)}
                    cy={PAD.top + toSvgY(last.y)}
                    r={3}
                    fill={s.color}
                  />
                );
              })()}
            </React.Fragment>
          );
        })}
      </Svg>

      {/* Legend */}
      {series.some(s => s.label) && (
        <View style={styles.legend}>
          {series.filter(s => s.label).map((s, i) => (
            <View key={i} style={styles.legendItem}>
              <View style={[styles.legendLine, { backgroundColor: s.color, borderStyle: s.dashed ? 'dashed' : 'solid' }]} />
              <Text style={styles.legendText}>{s.label}</Text>
            </View>
          ))}
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  legend: {
    flexDirection: 'row',
    gap: SPACING.md,
    justifyContent: 'center',
    marginTop: SPACING.xs,
  },
  legendItem: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
  },
  legendLine: {
    width: 18,
    height: 2,
    borderRadius: 1,
  },
  legendText: {
    color: COLORS.textMuted,
    fontSize: 10,
  },
});
