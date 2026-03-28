import React from 'react';
import { View, Text, StyleSheet, Image } from 'react-native';
import { ClassificacaoRow } from '../api/client';
import { COLORS, SPACING, RADIUS } from '../theme';

interface Props {
  row: ClassificacaoRow;
  index: number;
}

const SHIELD_PLACEHOLDER = 'https://s.glbimg.com/es/sde/f/2019/04/01/cartola_icon.png';

export default function StandingsRow({ row, index }: Props) {
  const isEven = index % 2 === 0;
  const zoneColor = row.cor_situacao || COLORS.textMuted;

  return (
    <View style={[styles.row, isEven && styles.rowEven]}>
      {/* Zone indicator */}
      <View style={[styles.zoneBar, { backgroundColor: zoneColor }]} />

      {/* Position */}
      <Text style={[styles.pos, getPositionStyle(row.posicao)]}>{row.posicao}</Text>

      {/* Shield + Name */}
      <Image
        source={{ uri: row.escudo_url || SHIELD_PLACEHOLDER }}
        style={styles.shield}
        resizeMode="contain"
      />
      <Text style={styles.name} numberOfLines={1}>{row.abreviacao}</Text>

      {/* Stats */}
      <View style={styles.statsGroup}>
        <StatCell label="Pts" value={row.pontos} highlight />
        <StatCell label="J" value={row.jogos} />
        <StatCell label="V" value={row.v} color={COLORS.primary} />
        <StatCell label="E" value={row.e} color={COLORS.warning} />
        <StatCell label="D" value={row.d} color={COLORS.danger} />
        <StatCell label="SG" value={row.sg} color={row.sg >= 0 ? COLORS.primary : COLORS.danger} />
        <StatCell label="%" value={row.aproveitamento ? `${row.aproveitamento}` : '—'} />
      </View>
    </View>
  );
}

function StatCell({ label, value, highlight, color }: {
  label: string;
  value: number | string;
  highlight?: boolean;
  color?: string;
}) {
  return (
    <View style={styles.statCell}>
      <Text style={[
        styles.statValue,
        highlight && styles.statHighlight,
        color ? { color } : null,
      ]}>
        {value ?? '—'}
      </Text>
    </View>
  );
}

function getPositionStyle(pos: number) {
  if (pos <= 4) return { color: COLORS.libertadores };
  if (pos <= 6) return { color: COLORS.preLibertadores };
  if (pos <= 12) return { color: COLORS.sulAmericana };
  if (pos >= 17) return { color: COLORS.danger };
  return { color: COLORS.textMuted };
}

const styles = StyleSheet.create({
  row: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 10,
    paddingRight: SPACING.md,
    backgroundColor: COLORS.bgCard,
  },
  rowEven: {
    backgroundColor: COLORS.bgCardAlt,
  },
  zoneBar: {
    width: 3,
    height: '100%',
    marginRight: 6,
    borderRadius: RADIUS.full,
  },
  pos: {
    width: 24,
    textAlign: 'center',
    fontSize: 13,
    fontWeight: '700',
    color: COLORS.textSecondary,
  },
  shield: {
    width: 22,
    height: 22,
    marginHorizontal: 6,
  },
  name: {
    flex: 1,
    color: COLORS.text,
    fontSize: 13,
    fontWeight: '600',
  },
  statsGroup: {
    flexDirection: 'row',
    gap: 2,
  },
  statCell: {
    width: 30,
    alignItems: 'center',
  },
  statValue: {
    color: COLORS.textSecondary,
    fontSize: 12,
  },
  statHighlight: {
    color: COLORS.text,
    fontWeight: '700',
    fontSize: 13,
  },
});
