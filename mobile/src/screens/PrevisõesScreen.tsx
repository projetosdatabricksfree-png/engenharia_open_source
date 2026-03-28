import React from 'react';
import {
  View, Text, FlatList, StyleSheet,
  RefreshControl, ActivityIndicator, StatusBar,
} from 'react-native';
import { LinearGradient } from 'expo-linear-gradient';
import { useSafeAreaInsets } from 'react-native-safe-area-context';
import { Ionicons } from '@expo/vector-icons';
import { useHomeData } from '../hooks/useData';
import MatchCard from '../components/MatchCard';
import StatCard from '../components/StatCard';
import { COLORS, SPACING } from '../theme';
import type { Previsao } from '../api/client';

// ── Sub-components ─────────────────────────────────────────────────────────

function LiveBadge() {
  return (
    <View style={styles.liveIndicator}>
      <View style={styles.liveDot} />
      <Text style={styles.liveText}>LIVE</Text>
    </View>
  );
}

function ErrorState({ message }: { message: string }) {
  return (
    <View style={styles.center}>
      <Ionicons name="wifi-outline" size={48} color={COLORS.danger} />
      <Text style={styles.errorTitle}>Sem conexão</Text>
      <Text style={styles.errorText}>{message}</Text>
    </View>
  );
}

// ── Main Screen ────────────────────────────────────────────────────────────

export default function PrevisõesScreen() {
  const insets = useSafeAreaInsets();
  const { previsoes, resumo, loading, refreshing, error, refresh } = useHomeData();

  // Unique rounds from the predictions using Set + toSorted (ES2023)
  const rounds = [...new Set(previsoes?.map(m => m.rodada) ?? [])].toSorted((a, b) => a - b);
  const currentRound = rounds.at(0); // ES2023: Array.at()

  const renderHeader = () => (
    <View>
      <LinearGradient
        colors={['#0A0E1A', '#0D1F2D', '#0A0E1A']}
        style={[styles.hero, { paddingTop: insets.top + SPACING.md }]}
      >
        {/* Title row */}
        <View style={styles.heroInner}>
          <View style={styles.heroTitle}>
            <View style={styles.logoBadge}>
              <Text style={styles.logoIcon}>⚽</Text>
            </View>
            <View>
              <Text style={styles.appTitle}>BrasileirãoPRO</Text>
              <Text style={styles.appSubtitle}>Previsões com IA · Série A 2026</Text>
            </View>
          </View>
          <LiveBadge />
        </View>

        {/* KPIs */}
        {resumo && (
          <View style={styles.kpiRow}>
            <StatCard
              label="Acurácia Geral"
              value={resumo.acuracia_geral != null ? `${resumo.acuracia_geral}%` : '—'}
              icon="analytics"
              color={COLORS.primary}
              sublabel="histórico"
            />
            <View style={{ width: SPACING.sm }} />
            <StatCard
              label="Próx. Partidas"
              value={resumo.proximas_partidas}
              icon="calendar"
              color={COLORS.gold}
            />
            <View style={{ width: SPACING.sm }} />
            <StatCard
              label="Previsões"
              value={resumo.total_previsoes_historico ?? '—'}
              icon="trophy"
              color={COLORS.info}
            />
          </View>
        )}
      </LinearGradient>

      <View style={styles.sectionHeader}>
        <Ionicons name="flash" size={16} color={COLORS.primary} />
        <Text style={styles.sectionTitle}>
          Próximas Partidas
          {currentRound != null && (
            <Text style={styles.sectionSub}> · Rodada {currentRound}</Text>
          )}
        </Text>
      </View>
    </View>
  );

  if (loading) {
    return (
      <View style={styles.center}>
        <ActivityIndicator size="large" color={COLORS.primary} />
        <Text style={styles.loadingText}>Carregando previsões...</Text>
      </View>
    );
  }

  if (error && !previsoes?.length) {
    return <ErrorState message={error} />;
  }

  return (
    <View style={styles.container}>
      <StatusBar barStyle="light-content" backgroundColor={COLORS.bg} />
      <FlatList<Previsao>
        data={previsoes ?? []}
        keyExtractor={item => String(item.id)}
        renderItem={({ item }) => <MatchCard match={item} />}
        ListHeaderComponent={renderHeader}
        ListEmptyComponent={
          <View style={styles.empty}>
            <Text style={styles.emptyText}>Nenhuma partida encontrada</Text>
          </View>
        }
        contentContainerStyle={styles.list}
        refreshControl={
          <RefreshControl
            refreshing={refreshing}
            onRefresh={refresh}
            tintColor={COLORS.primary}
            colors={[COLORS.primary]}
          />
        }
        showsVerticalScrollIndicator={false}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.bg },
  list: { paddingBottom: SPACING.xl },
  hero: { paddingHorizontal: SPACING.md, paddingBottom: SPACING.lg },
  heroInner: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: SPACING.lg,
  },
  heroTitle: { flexDirection: 'row', alignItems: 'center', gap: SPACING.sm },
  logoBadge: {
    width: 44, height: 44,
    borderRadius: 12,
    backgroundColor: 'rgba(0,210,106,0.15)',
    justifyContent: 'center', alignItems: 'center',
  },
  logoIcon: { fontSize: 22 },
  appTitle: {
    color: COLORS.text, fontSize: 20, fontWeight: '800', letterSpacing: -0.5,
  },
  appSubtitle: { color: COLORS.textSecondary, fontSize: 12 },
  liveIndicator: {
    flexDirection: 'row', alignItems: 'center', gap: 5,
    backgroundColor: 'rgba(0,210,106,0.12)',
    paddingHorizontal: SPACING.sm, paddingVertical: 4,
    borderRadius: 999, borderWidth: 1,
    borderColor: 'rgba(0,210,106,0.3)',
  },
  liveDot: { width: 6, height: 6, borderRadius: 3, backgroundColor: COLORS.primary },
  liveText: { color: COLORS.primary, fontSize: 10, fontWeight: '800', letterSpacing: 1.5 },
  kpiRow: { flexDirection: 'row' },
  sectionHeader: {
    flexDirection: 'row', alignItems: 'center', gap: SPACING.xs,
    paddingHorizontal: SPACING.md, paddingVertical: SPACING.md,
  },
  sectionTitle: { color: COLORS.text, fontSize: 16, fontWeight: '700' },
  sectionSub: { color: COLORS.textMuted, fontWeight: '400' },
  center: {
    flex: 1, backgroundColor: COLORS.bg,
    justifyContent: 'center', alignItems: 'center',
    gap: SPACING.sm, padding: SPACING.xl,
  },
  loadingText: { color: COLORS.textSecondary, fontSize: 14, marginTop: SPACING.sm },
  errorTitle: { color: COLORS.text, fontSize: 20, fontWeight: '700', marginTop: SPACING.sm },
  errorText: { color: COLORS.danger, fontSize: 13, textAlign: 'center' },
  empty: { padding: SPACING.xl, alignItems: 'center' },
  emptyText: { color: COLORS.textMuted, fontSize: 14 },
});
