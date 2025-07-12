#!/usr/bin/env python3

import unittest
import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))

def run_unit_tests():
    """Unit testleri çalıştır"""
    print("\n🧪 Unit Testleri Çalıştırılıyor...")
    print("=" * 50)
    
    # Discover and run unit tests
    loader = unittest.TestLoader()
    suite = loader.discover('tests/unit', pattern='test_*.py')
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()

def run_integration_tests():
    """Integration testleri çalıştır"""
    print("\n🔗 Integration Testleri Çalıştırılıyor...")
    print("=" * 50)
    
    # Discover and run integration tests
    loader = unittest.TestLoader()
    suite = loader.discover('tests/integration', pattern='test_*.py')
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()

def run_all_tests():
    """Tüm testleri çalıştır"""
    print("\n🚀 Tüm Testler Çalıştırılıyor...")
    print("=" * 50)
    
    unit_success = run_unit_tests()
    integration_success = run_integration_tests()
    
    print("\n" + "=" * 50)
    print("📊 Test Sonuçları:")
    print(f"   Unit Tests: {'✅ BAŞARILI' if unit_success else '❌ BAŞARISIZ'}")
    print(f"   Integration Tests: {'✅ BAŞARILI' if integration_success else '❌ BAŞARISIZ'}")
    
    overall_success = unit_success and integration_success
    print(f"\n🎯 Genel Sonuç: {'✅ TÜM TESTLER BAŞARILI' if overall_success else '❌ BAZI TESTLER BAŞARISIZ'}")
    
    return overall_success

def main():
    """Ana fonksiyon"""
    if len(sys.argv) > 1:
        test_type = sys.argv[1].lower()
        
        if test_type == 'unit':
            success = run_unit_tests()
        elif test_type == 'integration':
            success = run_integration_tests()
        elif test_type == 'all':
            success = run_all_tests()
        else:
            print("❌ Geçersiz test tipi. Kullanım:")
            print("   python run_tests.py [unit|integration|all]")
            print("   Hiçbir argüman verilmezse tüm testler çalıştırılır.")
            sys.exit(1)
    else:
        success = run_all_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()