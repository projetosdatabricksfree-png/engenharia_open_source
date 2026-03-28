import React from 'react';
import { View, StyleSheet, Platform } from 'react-native';
import { NavigationContainer } from '@react-navigation/native';
import { createBottomTabNavigator } from '@react-navigation/bottom-tabs';
import { Ionicons } from '@expo/vector-icons';
import { enableScreens } from 'react-native-screens';
import { COLORS } from '../theme';
import PrevisõesScreen from '../screens/PrevisõesScreen';
import TabelaScreen from '../screens/TabelaScreen';
import MetricasScreen from '../screens/MetricasScreen';

// Required for react-navigation on both native and web
enableScreens(Platform.OS !== 'web');

const Tab = createBottomTabNavigator();

export default function AppNavigator() {
  return (
    <NavigationContainer>
      <Tab.Navigator
        screenOptions={({ route }) => ({
          headerShown: false,
          tabBarStyle: styles.tabBar,
          tabBarActiveTintColor: COLORS.primary,
          tabBarInactiveTintColor: COLORS.textMuted,
          tabBarLabelStyle: styles.tabLabel,
          tabBarIcon: ({ focused, color }) => {
            const icons: Record<string, [string, string]> = {
              Previsões: ['flash', 'flash-outline'],
              Tabela:    ['list',  'list-outline'],
              Métricas:  ['analytics', 'analytics-outline'],
            };
            const [active, inactive] = icons[route.name] ?? ['help', 'help-outline'];
            return (
              <View style={focused ? styles.activeIcon : undefined}>
                <Ionicons
                  name={(focused ? active : inactive) as any}
                  size={focused ? 22 : 20}
                  color={color}
                />
              </View>
            );
          },
        })}
      >
        <Tab.Screen name="Previsões" component={PrevisõesScreen} />
        <Tab.Screen name="Tabela"    component={TabelaScreen} />
        <Tab.Screen name="Métricas"  component={MetricasScreen} />
      </Tab.Navigator>
    </NavigationContainer>
  );
}

const styles = StyleSheet.create({
  tabBar: {
    backgroundColor: '#0D1421',
    borderTopColor: '#1E2D45',
    borderTopWidth: 1,
    height: 60,
    paddingBottom: 8,
    paddingTop: 4,
  },
  tabLabel: {
    fontSize: 11,
    fontWeight: '600',
    letterSpacing: 0.2,
  },
  activeIcon: {
    backgroundColor: 'rgba(0,210,106,0.12)',
    borderRadius: 10,
    padding: 4,
    paddingHorizontal: 10,
  },
});
