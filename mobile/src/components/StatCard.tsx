import React from 'react';
import { View, Text, StyleSheet } from 'react-native';
import { LinearGradient } from 'expo-linear-gradient';
import { Ionicons } from '@expo/vector-icons';
import { COLORS, SPACING, RADIUS, SHADOWS } from '../theme';

interface Props {
  label: string;
  value: string | number;
  icon: string;
  color?: string;
  sublabel?: string;
}

export default function StatCard({ label, value, icon, color = COLORS.primary, sublabel }: Props) {
  return (
    <View style={[styles.card, SHADOWS.card]}>
      <LinearGradient
        colors={[`${color}22`, 'transparent']}
        style={styles.gradient}
        start={{ x: 0, y: 0 }}
        end={{ x: 1, y: 1 }}
      />
      <View style={[styles.iconBox, { backgroundColor: `${color}22` }]}>
        <Ionicons name={icon as any} size={20} color={color} />
      </View>
      <Text style={styles.value}>{value}</Text>
      <Text style={styles.label}>{label}</Text>
      {sublabel ? <Text style={styles.sublabel}>{sublabel}</Text> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    flex: 1,
    backgroundColor: COLORS.bgCard,
    borderRadius: RADIUS.lg,
    borderWidth: 1,
    borderColor: COLORS.bgBorder,
    padding: SPACING.md,
    overflow: 'hidden',
    gap: SPACING.xs,
  },
  gradient: {
    ...StyleSheet.absoluteFillObject,
  },
  iconBox: {
    width: 38,
    height: 38,
    borderRadius: RADIUS.md,
    justifyContent: 'center',
    alignItems: 'center',
    marginBottom: 4,
  },
  value: {
    color: COLORS.text,
    fontSize: 22,
    fontWeight: '800',
    letterSpacing: -0.5,
  },
  label: {
    color: COLORS.textSecondary,
    fontSize: 12,
    fontWeight: '500',
  },
  sublabel: {
    color: COLORS.textMuted,
    fontSize: 10,
  },
});
