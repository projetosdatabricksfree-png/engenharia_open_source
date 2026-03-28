import React from 'react';
import { View, Text, StyleSheet, Image } from 'react-native';
import { LinearGradient } from 'expo-linear-gradient';
import { Ionicons } from '@expo/vector-icons';
import { Previsao } from '../api/client';
import { COLORS, SPACING, RADIUS, SHADOWS } from '../theme';
import ProbabilityBar from './ProbabilityBar';

interface Props {
  match: Previsao;
}

const PREDICTION_LABEL: Record<string, string> = {
  casa: 'Vitória Casa',
  empate: 'Empate',
  visitante: 'Vitória Visitante',
};

const SHIELD_PLACEHOLDER = 'https://s.glbimg.com/es/sde/f/2019/04/01/cartola_icon.png';

export default function MatchCard({ match }: Props) {
  const confidence = match.confianca_pct ?? 0;
  const confidenceColor =
    confidence >= 70 ? COLORS.primary :
    confidence >= 50 ? COLORS.warning :
    COLORS.danger;

  return (
    <View style={[styles.card, SHADOWS.card]}>
      {/* Header: round + confidence */}
      <View style={styles.header}>
        <View style={styles.roundBadge}>
          <Text style={styles.roundText}>Rodada {match.rodada}</Text>
        </View>
        <View style={[styles.confBadge, { borderColor: confidenceColor }]}>
          <Ionicons name="flash" size={10} color={confidenceColor} />
          <Text style={[styles.confText, { color: confidenceColor }]}>
            {confidence.toFixed(1)}% conf.
          </Text>
        </View>
      </View>

      {/* Teams */}
      <View style={styles.teamsRow}>
        {/* Home */}
        <View style={styles.teamBlock}>
          <Image
            source={{ uri: match.escudo_casa || SHIELD_PLACEHOLDER }}
            style={styles.shield}
            resizeMode="contain"
          />
          <Text style={styles.teamAbrev}>{match.abrev_casa}</Text>
          <Text style={styles.teamName} numberOfLines={1}>{match.nome_casa}</Text>
          <View style={styles.statRow}>
            <Text style={styles.statLabel}>ELO</Text>
            <Text style={styles.statValue}>{match.elo_casa ?? '—'}</Text>
          </View>
          <View style={styles.statRow}>
            <Text style={styles.statLabel}>Pts</Text>
            <Text style={styles.statValue}>{match.pontos_casa ?? '—'}</Text>
          </View>
        </View>

        {/* VS divider */}
        <View style={styles.vsDivider}>
          <LinearGradient
            colors={['transparent', COLORS.bgBorder, 'transparent']}
            style={styles.dividerLine}
          />
          <Text style={styles.vsText}>VS</Text>
          <LinearGradient
            colors={['transparent', COLORS.bgBorder, 'transparent']}
            style={styles.dividerLine}
          />
        </View>

        {/* Away */}
        <View style={[styles.teamBlock, styles.teamRight]}>
          <Image
            source={{ uri: match.escudo_visitante || SHIELD_PLACEHOLDER }}
            style={styles.shield}
            resizeMode="contain"
          />
          <Text style={styles.teamAbrev}>{match.abrev_visitante}</Text>
          <Text style={styles.teamName} numberOfLines={1}>{match.nome_visitante}</Text>
          <View style={[styles.statRow, { justifyContent: 'flex-end' }]}>
            <Text style={styles.statLabel}>ELO</Text>
            <Text style={styles.statValue}>{match.elo_visitante ?? '—'}</Text>
          </View>
          <View style={[styles.statRow, { justifyContent: 'flex-end' }]}>
            <Text style={styles.statLabel}>Pts</Text>
            <Text style={styles.statValue}>{match.pontos_visitante ?? '—'}</Text>
          </View>
        </View>
      </View>

      {/* Probability bars */}
      <View style={styles.probSection}>
        <ProbabilityBar
          homePct={match.prob_casa_pct}
          drawPct={match.prob_empate_pct}
          awayPct={match.prob_visitante_pct}
          predicted={match.previsao}
        />
      </View>

      {/* Prediction footer */}
      <LinearGradient
        colors={['transparent', 'rgba(0,210,106,0.08)']}
        style={styles.footer}
      >
        <Ionicons name="trending-up" size={14} color={COLORS.primary} />
        <Text style={styles.footerText}>
          Previsão:{' '}
          <Text style={styles.footerHighlight}>
            {PREDICTION_LABEL[match.previsao] ?? match.previsao}
          </Text>
        </Text>
        <Text style={styles.modelBadge}>{match.modelo_versao}</Text>
      </LinearGradient>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: COLORS.bgCard,
    borderRadius: RADIUS.lg,
    borderWidth: 1,
    borderColor: COLORS.bgBorder,
    marginHorizontal: SPACING.md,
    marginBottom: SPACING.md,
    overflow: 'hidden',
  },
  header: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingHorizontal: SPACING.md,
    paddingTop: SPACING.md,
    paddingBottom: SPACING.sm,
  },
  roundBadge: {
    backgroundColor: 'rgba(0,210,106,0.12)',
    paddingHorizontal: SPACING.sm,
    paddingVertical: 3,
    borderRadius: RADIUS.full,
  },
  roundText: {
    color: COLORS.primary,
    fontSize: 11,
    fontWeight: '700',
    letterSpacing: 0.5,
  },
  confBadge: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 3,
    borderWidth: 1,
    paddingHorizontal: SPACING.sm,
    paddingVertical: 3,
    borderRadius: RADIUS.full,
  },
  confText: {
    fontSize: 11,
    fontWeight: '600',
  },
  teamsRow: {
    flexDirection: 'row',
    paddingHorizontal: SPACING.md,
    paddingBottom: SPACING.sm,
    alignItems: 'flex-start',
  },
  teamBlock: {
    flex: 1,
    alignItems: 'flex-start',
    gap: 2,
  },
  teamRight: {
    alignItems: 'flex-end',
  },
  shield: {
    width: 44,
    height: 44,
    marginBottom: 4,
  },
  teamAbrev: {
    color: COLORS.text,
    fontSize: 18,
    fontWeight: '800',
    letterSpacing: 0.5,
  },
  teamName: {
    color: COLORS.textSecondary,
    fontSize: 11,
    maxWidth: 110,
  },
  statRow: {
    flexDirection: 'row',
    gap: 4,
    alignItems: 'center',
    marginTop: 2,
  },
  statLabel: {
    color: COLORS.textMuted,
    fontSize: 10,
    letterSpacing: 0.5,
    textTransform: 'uppercase',
  },
  statValue: {
    color: COLORS.textSecondary,
    fontSize: 11,
    fontWeight: '600',
  },
  vsDivider: {
    width: 40,
    alignItems: 'center',
    justifyContent: 'center',
    gap: SPACING.xs,
    paddingTop: 12,
  },
  dividerLine: {
    width: 1,
    height: 20,
  },
  vsText: {
    color: COLORS.textMuted,
    fontSize: 11,
    fontWeight: '700',
    letterSpacing: 2,
  },
  probSection: {
    paddingHorizontal: SPACING.md,
    paddingBottom: SPACING.md,
  },
  footer: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: SPACING.xs,
    paddingHorizontal: SPACING.md,
    paddingVertical: SPACING.sm,
    borderTopWidth: 1,
    borderTopColor: COLORS.bgBorder,
  },
  footerText: {
    color: COLORS.textSecondary,
    fontSize: 12,
    flex: 1,
  },
  footerHighlight: {
    color: COLORS.primary,
    fontWeight: '700',
  },
  modelBadge: {
    color: COLORS.textMuted,
    fontSize: 10,
    letterSpacing: 0.3,
  },
});
